import errno
from hashlib import md5
from multiprocessing.dummy import Pool
import os
import json
import requests
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time

from collections import namedtuple
from distutils.version import LooseVersion

from metaflow.datastore import DATASTORES
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    CONDA_DEPENDENCY_RESOLVER,
    CONDA_LOCAL_DIST_DIRNAME,
    CONDA_LOCAL_DIST,
    CONDA_LOCAL_PATH,
    CONDA_LOCK_TIMEOUT,
    CONDA_REMOTE_INSTALLER,
    CONDA_REMOTE_INSTALLER_DIRNAME,
)
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.plugins.conda import arch_id, get_conda_root, get_conda_package_root
from metaflow.util import which

_CONDA_DEP_RESOLVERS = ("conda", "mamba")


LazyFetchResult = namedtuple(
    "LazyFetchResult", "filename url cache_url local_path fetched cached is_dir"
)

TransmuteResult = namedtuple("TransmuteResult", "orig_path new_path new_hash error")


def _get_md5_hash(path):
    md5_hash = md5()
    with open(path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)
    return md5_hash.hexdigest()


class CondaException(MetaflowException):
    headline = "Conda ran into an error while setting up environment."

    def __init__(self, error):
        if isinstance(error, (list,)):
            error = "\n".join(error)
        msg = "{error}".format(error=error)
        super(CondaException, self).__init__(msg)


class CondaStepException(CondaException):
    def __init__(self, exception, steps):
        msg = "Step(s): {steps}, Error: {error}".format(
            steps=steps, error=exception.message
        )
        super(CondaStepException, self).__init__(msg)


class Conda(object):
    def __init__(self, echo, datastore_type, mode="local"):
        from metaflow.cli import logger

        if id(echo) != id(logger):

            def _modified_logger(*args, **kwargs):
                if "timestamp" in kwargs:
                    del kwargs["timestamp"]
                echo(*args, **kwargs)

            self._echo = _modified_logger
        else:
            self._echo = echo

        self._cached_info = None
        self._datastore_type = datastore_type
        self._mode = mode
        self._bins = self._dependency_solver = None
        self._have_micromamba = False  # True if the installer is micromamba
        self._resolve_conda_binary()

    def resolve(self, using_steps, env_id, deps, channels, architecture):
        if self._mode != "local":
            # TODO: This will change and we will need to "upgrade" if needed but for
            # now, we just punt on resolving an environment remotely -- it should already
            # have been resolved.
            raise CondaException("Cannot resolve environments in a remote environment")

        try:
            # We resolve the environment using conda-lock

            # Write out the requirement yml file. It's easy enough so don't use a YAML
            # library to avoid adding another dep

            # Add channels
            lines = ["channels:\n"]
            lines.extend(["  - %s" % c for c in channels])
            lines.append("  - conda-forge\n")

            # Add deps
            lines.append("dependencies:\n")
            lines.extend(["  - %s\n" % d.decode("ascii") for d in deps])

            with tempfile.NamedTemporaryFile(mode="w", encoding="ascii") as input_yml:
                input_yml.writelines(lines)
                input_yml.flush()
                args = [
                    "lock",
                    "-f",
                    input_yml.name,
                    "-p",
                    architecture,
                    "--filename-template",
                    "conda-lock-gen-%s-{platform}" % env_id,
                    "-k",
                    "explicit",
                    "--conda",
                    self._bins.get("micromamba", self._bins["conda"]),
                ]
                if "micromamba" in self._bins:
                    args.append("--micromamba")
                elif self._dependency_solver == "mamba":
                    args.append("--mamba")
                self._call_conda(args, binary="conda-lock")
            # At this point, we need to read the explicit dependencies in the file created
            emit = False
            result = []
            with open(
                "conda-lock-gen-%s-%s" % (env_id, architecture), "r", encoding="utf-8"
            ) as out:
                for l in out:
                    if emit:
                        result.append(l.strip())
                    if not emit and l.strip() == "@EXPLICIT":
                        emit = True
            return result
        except CondaException as e:
            raise CondaStepException(e, using_steps)
        finally:
            if os.path.isfile("conda-lock-gen-%s-%s" % (env_id, architecture)):
                os.unlink("conda-lock-gen-%s-%s" % (env_id, architecture))

    def create(self, step_name, env_id, env_desc, do_symlink=False):
        # env_desc is a DS that is stored in the CONDA_MAGIC_FILE and contains the
        # following fields:
        # deps: Explicit dependencies that are needed
        # channels: Additional channels
        # order: Name of the packages downloaded by urls
        # urls: URLs to download to satisfy all deps
        # hashes: MD5 hashes of the packages at those URLs
        # cache_urls: (optional) A cached version of those packages -- prefer this
        # if it exists
        try:
            # I am not 100% sure the lock is required but since the environments share
            # a common package cache, we will keep it for now
            with CondaLock(self._env_lock_file(env_id)):
                self._create(env_id, env_desc)
            if do_symlink:
                os.symlink(
                    self.python(env_id), os.path.join(os.getcwd(), "__conda_python")
                )
        except CondaException as e:
            raise CondaStepException(e, step_name)

    def remove(self, step_name, env_id):
        # Remove the conda environment
        try:
            with CondaLock(self._env_lock_file(env_id)):
                self._remove(env_id)
        except CondaException as e:
            raise CondaStepException(e, step_name)

    def python(self, env_id):
        # Get Python interpreter for the conda environment
        return os.path.join(self._env_path(env_id), "bin/python")

    def environments(self, flowname):
        # List all conda environments associated with the flow
        ret = {}
        if self._have_micromamba:
            env_dir = os.path.join(self._info["base environment"], "envs")
            for entry in os.scandir(env_dir):
                if entry.is_dir() and entry.name.startswith("metaflow_%s" % flowname):
                    ret[entry.name] = entry.path
        else:
            envs = self._info["envs"]
            for env in envs:
                # Named environments are always $CONDA_PREFIX/envs/
                if "/envs/" in env:
                    name = os.path.basename(env)
                    if name.startswith("metaflow_%s" % flowname):
                        ret[name] = env
        return ret

    def lazy_fetch_packages(
        self,
        filenames,
        urls,
        cache_urls,
        file_hashes,
        require_tarball=False,
        requested_arch=arch_id(),
        tempdir=None,
    ):
        # Lazily fetch filenames into the pkgs directory.
        # filenames, urls, cache_urls, file_hashes are all arrays of the same size with
        # a 1:1 correspondance between them
        #  - If the file exists as a tarball (or as a directory if require_tarball is False),
        #    do nothing.
        #  - If the file does not exist:
        #    - if a cache_url exists, fetch that
        #    - if not, fetch from urls
        #
        # Returns a list of tuple: (fetched, filename, url/cache_url, local_path) where:
        #  - fetched is a boolean indicating if we needed to fetch the file
        #  - filename is the filename
        #  -
        # is None if not fetched

        def _download_web(entry):
            url, local_path = entry
            try:
                with requests.get(url, stream=True) as r:
                    with open(local_path, "wb") as f:
                        # TODO: We could check the hash here
                        shutil.copyfileobj(r.raw, f)
            except Exception as e:
                return (url, e)
            return None

        results = []
        use_package_dirs = True
        if requested_arch != arch_id():
            if tempdir is None:
                raise MetaflowInternalError(
                    "Cannot lazily fetch packages for another architecture "
                    "without a temporary directory"
                )
            use_package_dirs = False

        cache_downloads = []
        web_downloads = []
        url_adds = []
        known_urls = set()
        if use_package_dirs:
            package_dirs = self._package_dirs
            for p in package_dirs:
                with CondaLock(self._package_dir_lock_file(p)):
                    url_file = os.path.join(p, "urls.txt")
                    if os.path.isfile(url_file):
                        with open(url_file, "rb") as f:
                            known_urls.update([l.strip().decode("utf-8") for l in f])
        else:
            package_dirs = [tempdir]

        for filename, base_url, cache_url, file_hash in zip(
            filenames, urls, cache_urls, file_hashes
        ):
            for p in [d for d in package_dirs if os.path.isdir(d)]:
                path = os.path.join(p, filename)
                extract_path = None
                if not require_tarball:
                    if filename.endswith(".tar.bz2"):
                        extract_path = os.path.join(p, filename[:-8])
                    elif filename.endswith(".conda"):
                        extract_path = os.path.join(p, filename[:-5])

                if extract_path and os.path.isdir(extract_path):
                    print("Found directory: %s" % extract_path)
                    results.append(
                        LazyFetchResult(
                            filename=filename,
                            url=base_url,
                            cache_url=cache_url,
                            local_path=extract_path,
                            fetched=False,
                            cached=False,
                            is_dir=True,
                        )
                    )
                    break
                elif os.path.isfile(path):
                    print("Found file %s" % path)
                    md5_hash = _get_md5_hash(path)
                    if md5_hash == file_hash:
                        results.append(
                            LazyFetchResult(
                                filename=filename,
                                url=base_url,
                                cache_url=cache_url,
                                local_path=path,
                                fetched=False,
                                cached=False,
                                is_dir=False,
                            )
                        )
                        break
                    else:
                        print(
                            "Hash computed as %s -- expected %s" % (md5_hash, file_hash)
                        )
            else:
                # We need to download this file; check if it is a regular download
                # or one from the cache
                if base_url not in known_urls:
                    # In some cases the url is in urls.txt but the package has been
                    # cleaned up. In other words, urls.txt does not seem to be kept up
                    # to date -- it's like an append only thing.
                    url_adds.append("%s\n" % base_url)
                if cache_url:
                    cache_downloads.append(
                        (cache_url, os.path.join(package_dirs[0], filename))
                    )
                    results.append(
                        LazyFetchResult(
                            filename=filename,
                            url=base_url,
                            cache_url=cache_url,
                            local_path=os.path.join(package_dirs[0], filename),
                            fetched=True,
                            cached=True,
                            is_dir=False,
                        )
                    )
                else:
                    web_downloads.append(
                        (base_url, os.path.join(package_dirs[0], filename))
                    )
                    results.append(
                        LazyFetchResult(
                            filename=filename,
                            url=base_url,
                            cache_url=cache_url,
                            local_path=os.path.join(package_dirs[0], filename),
                            fetched=True,
                            cached=False,
                            is_dir=False,
                        )
                    )
        do_download = web_downloads or cache_downloads
        if do_download:
            start = time.time()
            self._echo(
                "    Downloading %d(web) + %d(cache) packages out of %d  ..."
                % (len(web_downloads), len(cache_downloads), len(filenames)),
                nl=False,
            )

        # Ensure the packages directory exists at the very least
        if do_download and not os.path.isdir(package_dirs[0]):
            os.makedirs(package_dirs[0])

        # Could parallelize this again but unlikely to see a huge gain
        if web_downloads:
            errors = [
                r
                for r in Pool().imap_unordered(_download_web, web_downloads)
                if r is not None
            ]
            if errors:
                raise CondaException("Error downloading packages: %s" % str(errors))

        if cache_downloads:
            conda_package_root = get_conda_package_root(self._datastore_type)
            storage = DATASTORES[self._datastore_type](conda_package_root)
            errors = []
            with storage.load_bytes([x[0] for x in cache_downloads]) as load_results:
                for (key, tmpfile, _), local_file in zip(
                    load_results, [x[1] for x in cache_downloads]
                ):
                    if not tmpfile:
                        errors.append(key)
                    shutil.move(tmpfile, local_file)
            if errors:
                raise CondaException(
                    "Could not download the following cached packages (missing): %s"
                    % str(errors)
                )
        if url_adds:
            # Update the urls file in the packages directory so that Conda knows that the
            # files are there
            with CondaLock(self._package_dir_lock_file(package_dirs[0])):
                with open(
                    os.path.join(package_dirs[0], "urls.txt"),
                    mode="a",
                    encoding="utf-8",
                ) as f:
                    f.writelines(url_adds)
        if do_download:
            self._echo("  done in %d seconds." % int(time.time() - start))
        return results

    def transmute_packages(
        self,
        orig_paths,
        orig_urls,
        new_format=".conda",
        replace_origs=False,
        output_dir=None,
    ):
        # Transmute packages to new_format.
        # - replace_origs: If True, replace old packages with the new ones and
        #   update urls.txt if needed
        # - output_dir: If set, use this directory for new packages. If not specified,
        #   use the same directory.
        # Returns a list of TransmuteResult
        if new_format == ".conda":
            old_format = ".tar.bz2"
        else:
            old_format = ".conda"

        if "cph" not in self._bins:
            raise CondaException(
                "Cannot transmute packages without `cph`. "
                "Please install using `%s install -n base conda-package-handling"
                % self._dependency_solver
            )

        def _transmute(orig_path):
            args = ["t", "--processes", "1", "--force", "--output-dir"]
            old_name = os.path.basename(orig_path)
            if not old_name.endswith(old_format):
                return (
                    orig_path,
                    MetaflowInternalError(
                        "Package %s does not end in %s" % (orig_path, old_format)
                    ),
                )
            new_name = old_name[: -len(old_format)] + new_format
            if output_dir:
                args.append(output_dir)
                new_path = os.path.join(output_dir, new_name)
            else:
                args.append(os.path.dirname(orig_path))
                new_path = os.path.join(os.path.dirname(orig_path), old_name)
            args.extend([orig_path, new_format])
            try:
                self._call_conda(args, binary="cph")
            except CondaException as e:
                return (orig_path, e)
            else:
                if replace_origs:
                    os.unlink(orig_path)
            return (orig_path, new_path)

        if output_dir is not None and replace_origs:
            raise MetaflowInternalError(
                "Cannot specify an output_dir and replace_origs in transmute_packages"
            )
        transmute_results = Pool().imap_unordered(_transmute, orig_paths)
        results = []
        # TODO: Update urls.txt removing the old ones and adding the new ones.
        for orig_path, result in transmute_results:
            if isinstance(result, Exception):
                results.append(
                    TransmuteResult(
                        orig_path=orig_path, new_path=None, new_hash=None, error=result
                    )
                )
            else:
                results.append(
                    TransmuteResult(
                        orig_path=orig_path,
                        new_path=result,
                        new_hash=_get_md5_hash(result),
                        error=None,
                    )
                )
        return results

    def _resolve_conda_binary(self):
        self._dependency_solver = CONDA_DEPENDENCY_RESOLVER.lower()
        if self._dependency_solver not in _CONDA_DEP_RESOLVERS:
            raise InvalidEnvironmentException(
                "Invalid Conda dependency resolver %s, valid candidates are %s."
                % (self._dependency_solver, _CONDA_DEP_RESOLVERS)
            )
        if self._mode == "local":
            self._resolve_local_conda()
        else:
            # Remote mode -- we install a conda environment or make sure we have
            # one already there
            self._resolve_remote_conda()

        err = self._validate_conda_installation()
        if err is not None:
            raise err

    def _resolve_local_conda(self):
        if CONDA_LOCAL_PATH is not None:
            # We need to look in a specific place
            self._bins = {
                "conda": os.path.join(CONDA_LOCAL_PATH, "bin", self._dependency_solver),
                "conda-lock": os.path.join(CONDA_LOCAL_PATH, "bin", "conda-lock"),
                "micromamba": os.path.join(CONDA_LOCAL_PATH, "bin", "micromamba"),
                "cph": os.path.join(CONDA_LOCAL_PATH, "bin", "cph"),
            }
            if self._validate_conda_installation():
                # This means we have an exception so we are going to try to install
                with CondaLock(
                    os.path.abspath(
                        os.path.join(CONDA_LOCAL_PATH, "..", ".conda-install.lock")
                    )
                ):
                    if self._validate_conda_installation():
                        self._install_local_conda()
        else:
            self._bins = {
                "conda": which(self._dependency_solver),
                "conda-lock": which("conda-lock"),
                "micromamba": which("micromamba"),
                "cph": which("cph"),
            }

    def _install_local_conda(self):
        start = time.time()
        path = CONDA_LOCAL_PATH
        self._echo("    Installing Conda environment at %s  ..." % path, nl=False)
        shutil.rmtree(CONDA_LOCAL_PATH, ignore_errors=True)

        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        path_to_fetch = os.path.join(
            CONDA_LOCAL_DIST_DIRNAME,
            CONDA_LOCAL_DIST.format(arch=arch_id()),
        )
        storage = DATASTORES[self._datastore_type](get_conda_root(self._datastore_type))
        with tempfile.NamedTemporaryFile() as tmp:
            with storage.load_bytes([path_to_fetch]) as load_results:
                for _, tmpfile, _ in load_results:
                    if tmpfile is None:
                        raise InvalidEnvironmentException(
                            msg="Cannot find Conda installation tarball '%s'"
                            % os.path.join(
                                get_conda_root(self._datastore_type), path_to_fetch
                            )
                        )
                    shutil.move(tmpfile, tmp.name)
            try:
                tar = tarfile.open(tmp.name)
                tar.extractall(path)
                tar.close()
            except Exception as e:
                raise InvalidEnvironmentException(
                    msg="Could not extract environment: %s" % str(e)
                )
        self._echo("  done in %d seconds." % int(time.time() - start), timestamp=False)

    def _resolve_remote_conda(self):
        if CONDA_REMOTE_INSTALLER is not None:
            self._install_remote_conda()
        else:
            # If we don't have a REMOTE_INSTALLER, we check if we need to install one
            args = [
                "if ! type %s  >/dev/null 2>&1; \
                then wget --no-check-certificate "
                "https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh -O Miniforge3.sh >/dev/null 2>&1; \
                bash ./Miniforge3.sh -b >/dev/null 2>&1; echo $HOME/miniforge3/bin/conda; "
                "else which %s; fi"
                % (self._dependency_solver, self._dependency_solver),
            ]
            self._bins = {"conda": subprocess.check_output(args)}

    def _install_remote_conda(self):
        # We download the installer and return a path to it
        final_path = os.path.join(os.getcwd(), "__conda_installer")
        from metaflow.datastore import DATASTORES

        path_to_fetch = os.path.join(
            CONDA_REMOTE_INSTALLER_DIRNAME,
            CONDA_REMOTE_INSTALLER.format(arch=arch_id()),
        )
        if self._datastore_type not in DATASTORES:
            raise MetaflowException(
                msg="Downloading conda remote installer from backend %s is unimplemented!"
                % self._datastore_type
            )
        storage = DATASTORES[self._datastore_type](get_conda_root(self._datastore_type))
        with storage.load_bytes([path_to_fetch]) as load_results:
            for _, tmpfile, _ in load_results:
                if tmpfile is None:
                    raise MetaflowException(
                        msg="Cannot find Conda remote installer '%s'"
                        % os.path.join(
                            get_conda_root(self._datastore_type), path_to_fetch
                        )
                    )
                shutil.move(tmpfile, final_path)
        os.chmod(
            final_path,
            stat.S_IRUSR
            | stat.S_IXUSR
            | stat.S_IRGRP
            | stat.S_IXGRP
            | stat.S_IROTH
            | stat.S_IXOTH,
        )
        self._bins = {"conda": final_path}

    def _validate_conda_installation(self):
        # Check if the dependency solver exists.
        to_remove = []
        for k, v in self._bins.items():
            if v is None or not os.path.isfile(v):
                if k == "conda":
                    return InvalidEnvironmentException(
                        "No %s installation found. Install %s first."
                        % (self._dependency_solver, self._dependency_solver)
                    )
                elif k in ("micromamba", "cph"):
                    # This is an optional install so we ignore if not present
                    to_remove.append(k)
                else:
                    return InvalidEnvironmentException(
                        "Required binary '%s' found. Install using `%s install -n base %s`"
                        % (k, self._dependency_solver, k)
                    )
        if to_remove:
            for k in to_remove:
                del self._bins[k]

        if "micromamba version" in self._info:
            self._have_micromamba = True
            if LooseVersion(self._info["micromamba version"]) < LooseVersion("0.25.1"):
                msg = "Microconda version 0.25.1 or newer is required."
                return InvalidEnvironmentException(msg)
        elif self._dependency_solver == "conda" or self._dependency_solver == "mamba":
            if LooseVersion(self._info["conda_version"]) < LooseVersion("4.6.0"):
                msg = "Conda version 4.6.0 or newer is required."
                if self._dependency_solver == "mamba":
                    msg += (
                        " Visit https://mamba.readthedocs.io/en/latest/installation.html "
                        "for installation instructions."
                    )
                else:
                    msg += (
                        " Visit https://docs.conda.io/en/latest/miniconda.html "
                        "for installation instructions."
                    )
                return InvalidEnvironmentException(msg)
        else:
            # Should never happen since we check for it but making it explicit
            raise InvalidEnvironmentException(
                "Unknown dependency solver: %s" % self._dependency_solver
            )

        if self._mode == "local":
            # Check if conda-forge is available as a channel to pick up Metaflow's
            # dependencies. This check will go away once all of Metaflow's
            # dependencies are vendored in.
            if "conda-forge" not in "\t".join(self._info["channels"]):
                return InvalidEnvironmentException(
                    "Conda channel 'conda-forge' is required. "
                    "Specify it with CONDA_CHANNELS environment variable."
                )

        return None

    @property
    def _package_dirs(self):
        info = self._info
        if self._have_micromamba:
            pkg_dir = os.path.join(info["base environment"], "pkgs")
            if not os.path.exists(pkg_dir):
                os.makedirs(pkg_dir)
            return [pkg_dir]
        return info["pkgs_dirs"]

    @property
    def _info(self):
        if self._cached_info is None:
            self._cached_info = json.loads(self._call_conda(["info", "--json"]))
        return self._cached_info

    def _create(self, env_id, env_desc):
        # We first get all the packages needed
        cache_urls = env_desc.get("cache_urls", [None] * len(env_desc["urls"]))
        self.lazy_fetch_packages(
            env_desc["order"], env_desc["urls"], cache_urls, env_desc["hashes"]
        )
        # At this point, we have all the packages that we need so we should be able to
        # just install directly
        start = time.time()
        self._echo("    Extracting and linking Conda environment ...", nl=False)
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="ascii", delete=False
        ) as explicit_list:
            # We create an explicit file
            lines = ["@EXPLICIT\n"]
            lines.extend(
                [
                    "%s#%s\n" % (base_url, file_hash)
                    for base_url, file_hash in zip(env_desc["urls"], env_desc["hashes"])
                ]
            )
            explicit_list.writelines(lines)
            explicit_list.flush()
            self._call_conda(
                [
                    "create",
                    "--yes",
                    "--quiet",
                    "--offline",
                    "--no-deps",
                    "--name",
                    env_id,
                    "--file",
                    explicit_list.name,
                ],
                # Creating with micromamba is faster as it extracts in parallel. Prefer
                # it if it exists.
                binary="micromamba" if "micromamba" in self._bins else "conda",
            )
        self._cached_info = None
        self._echo("  done in %s seconds." % int(time.time() - start), timestamp=False)

    def _remove(self, env_id):
        self._call_conda(["env", "remove", "--name", env_id, "--yes", "--quiet"])
        self._cached_info = None

    def _env_path(self, env_id):
        if self._have_micromamba:
            env_dir = os.path.join(self._info["base environment"], "envs")
            for entry in os.scandir(env_dir):
                if entry.is_dir() and entry.name == env_id:
                    return entry.path
        else:
            envs = self._info["envs"]
            for env in envs:
                if "/envs/" in env:
                    name = os.path.basename(env)
                    if name == env_id:
                        return env
        return None

    def _env_lock_file(self, env_id):
        if self._have_micromamba:
            return os.path.join(self._info["base environment"], "mf_env-creation.lock")

        return os.path.join(self._info["envs_dirs"][0], "mf_env-creation.lock")

    def _package_dir_lock_file(self, dir):
        return os.path.join(dir, "mf_pkgs-update.lock")

    def _call_conda(
        self, args, binary="conda", architecture=None, disable_safety_checks=False
    ):
        try:
            env = {
                "CONDA_JSON": "True",
                "CONDA_SUBDIR": (architecture if architecture else ""),
                "MAMBA_NO_BANNER": "1",
                "MAMBA_JSON": "True",
            }
            if disable_safety_checks:
                env["CONDA_SAFETY_CHECKS"] = "disabled"
            if binary == "micromamba":
                # Add a few options to make sure it plays well with conda/mamba
                # NOTE: This is only if we typically have conda/mamba and are using
                # micromamba. When micromamba is used by itself, we don't do this
                args.extend(["-r", os.path.dirname(self._package_dirs[0])])
            print("Calling %s" % str([self._bins[binary]] + args))
            return subprocess.check_output(
                [self._bins[binary]] + args,
                stderr=subprocess.PIPE,
                env=dict(os.environ, **env),
            ).strip()
        except subprocess.CalledProcessError as e:
            try:
                output = json.loads(e.output)
                err = [output["error"]]
                for error in output.get("errors", []):
                    err.append(error["error"])
                raise CondaException(err)
            except (TypeError, ValueError) as ve:
                pass
            raise CondaException(
                "Conda command '{cmd}' returned error ({code}): {output}, stderr={stderr}".format(
                    cmd=e.cmd, code=e.returncode, output=e.output, stderr=e.stderr
                )
            )


class CondaLock(object):
    def __init__(self, lock, timeout=CONDA_LOCK_TIMEOUT, delay=10):
        self.lock = lock
        self.locked = False
        self.timeout = timeout
        self.delay = delay

    def _acquire(self):
        start = time.time()
        try:
            os.makedirs(os.path.dirname(self.lock))
        except OSError as x:
            if x.errno != errno.EEXIST:
                raise
        while True:
            try:
                self.fd = os.open(self.lock, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                self.locked = True
                break
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                if self.timeout is None:
                    raise CondaException("Could not acquire lock {}".format(self.lock))
                if (time.time() - start) >= self.timeout:
                    raise CondaException(
                        "Timeout occurred while acquiring lock {}".format(self.lock)
                    )
                time.sleep(self.delay)

    def _release(self):
        if self.locked:
            os.close(self.fd)
            os.unlink(self.lock)
            self.locked = False

    def __enter__(self):
        if not self.locked:
            self._acquire()
        return self

    def __exit__(self, type, value, traceback):
        self.__del__()

    def __del__(self):
        self._release()
