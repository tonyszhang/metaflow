import errno
from hashlib import md5
from multiprocessing.dummy import Pool
import os
import json
import typing
import requests
import shutil
import stat
import subprocess
import tarfile
import tempfile
import time

from collections import namedtuple
from distutils.version import LooseVersion

from urllib.parse import urlparse

from metaflow.datastore import DATASTORES
from metaflow.debug import debug
from metaflow.exception import MetaflowException, MetaflowInternalError
from metaflow.metaflow_config import (
    CONDA_DEPENDENCY_RESOLVER,
    CONDA_LOCAL_DIST_DIRNAME,
    CONDA_LOCAL_DIST,
    CONDA_LOCAL_PATH,
    CONDA_LOCK_TIMEOUT,
    CONDA_REMOTE_INSTALLER,
    CONDA_REMOTE_INSTALLER_DIRNAME,
    CONDA_PREFERRED_FORMAT,
)
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.plugins.conda import arch_id, get_conda_root, get_conda_package_root
from metaflow.util import which

from . import get_md5_hash, CONDA_FORMATS, TRANSMUT_PATHCOMPONENT

_CONDA_DEP_RESOLVERS = ("conda", "mamba")

# per_format_desc is a dictionary containing cache_url, local_path,
# fetched, cached and transmuted
# Move to dataclasses for Py 3.7+
LazyFetchResult = namedtuple("LazyFetchResult", "filename url is_dir per_format_desc")

ParseURLResult = namedtuple("ParseURLResult", "filename format hash is_transmuted")


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
    @staticmethod
    def _convert_filepath(path, file_format=None):
        if file_format and not path.endswith(file_format):
            for f in CONDA_FORMATS:
                if path.endswith(f):
                    path = path[: -len(f)] + file_format
                    break
            else:
                raise CondaException(
                    "URL '%s' does not end with a supported file format %s"
                    % (path, str(CONDA_FORMATS))
                )
        return os.path.split(path)

    def _make_urlstxt_from_url(self, base_url, file_format=None, is_transmuted=False):
        if not is_transmuted:
            return base_url
        url = urlparse(base_url)
        file_path, filename = Conda._convert_filepath(url.path, file_format)
        return os.path.join(
            get_conda_package_root(self._datastore_type),
            TRANSMUT_PATHCOMPONENT,
            url.netloc,
            file_path.lstrip("/"),
            filename,
        )

    def _make_urlstxt_from_cacheurl(self, cache_url):
        if TRANSMUT_PATHCOMPONENT in cache_url:
            return os.path.join(
                get_conda_package_root(self._datastore_type),
                os.path.split(os.path.split(cache_url)[0])[
                    0
                ],  # Strip off last two (hash and filename)
            )
        else:
            return "https://" + os.path.split(os.path.split(cache_url)[0])[0]

    @staticmethod
    def make_cache_url(base_url, file_hash, file_format=None, is_transmuted=False):
        url = urlparse(base_url)
        file_path, filename = Conda._convert_filepath(url.path, file_format)

        if is_transmuted:
            return os.path.join(
                TRANSMUT_PATHCOMPONENT,
                url.netloc,
                file_path.lstrip("/"),
                filename,
                file_hash,
                filename,
            )
        else:
            return os.path.join(
                url.netloc, file_path.lstrip("/"), filename, file_hash, filename
            )

    @staticmethod
    def parse_cache_url(url):
        basename, filename = os.path.split(url)
        file_format = None
        for f in CONDA_FORMATS:
            if filename.endswith(f):
                file_format = f
                break
        else:
            raise CondaException(
                "URL '%s' does not end with a supported file format %s"
                % (url, str(CONDA_FORMATS))
            )
        basename, file_hash = os.path.split(basename)
        is_transmuted = TRANSMUT_PATHCOMPONENT in basename
        return ParseURLResult(
            filename=filename,
            format=file_format,
            hash=file_hash,
            is_transmuted=is_transmuted,
        )

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
        require_format=None,
        requested_arch=arch_id(),
        tempdir=None,
    ):
        # Lazily fetch filenames into the pkgs directory.
        # filenames, urls, cache_urls, file_hashes are all arrays of the same size with
        # a 1:1 correspondance between them
        #  - If the file exists in the all the required formats (or as a directory if
        #    require_format is None)
        #  - If the file does not exist:
        #    - if a cache_url exists, fetch that
        #    - if not, fetch from urls
        #
        # Returns a list of tuple: (fetched, filename, url/cache_url, local_path) where:
        #  - fetched is a boolean indicating if we needed to fetch the file
        #  - filename is the filename
        #  -
        # is None if not fetched

        results = []

        if require_format is None:
            require_format = []

        use_package_dirs = True
        if requested_arch != arch_id():
            if tempdir is None:
                raise MetaflowInternalError(
                    "Cannot lazily fetch packages for another architecture "
                    "without a temporary directory"
                )
            use_package_dirs = False

        cache_downloads = []  # Contains a list of (src_cache, filename, dst_local_file)
        web_downloads = []  # Contains a list of (filename, src_url, dst_local_file)
        # Contains a list of (filename, format, src_local_file, dst_local_file, urltxt_to_add)
        transmutes = []
        url_adds = []  # List of URLs to add to urls.txt
        known_urls = set()  # Set of URLs already in urls.txt

        # Helper functions
        def _add_to_downloads_list_and_results(filename, pkg_format, mode, src, dst):
            if mode == "cache":
                cache_downloads.append((src, filename, dst))
                return {
                    pkg_format: {
                        "cache_url": src,
                        "local_path": dst,
                        "fetched": True,
                        "cached": True,
                        "transmuted": False,
                    }
                }
            elif mode == "web":
                web_downloads.append((filename, src, dst))
                return {
                    pkg_format: {
                        "cache_url": None,
                        "local_path": dst,
                        "fetched": True,
                        "cached": False,
                        "transmuted": False,
                    }
                }
            elif mode == "local":
                return {
                    pkg_format: {
                        "cache_url": None,
                        "local_path": src,
                        "fetched": False,
                        "cached": False,
                        "transmuted": False,
                    }
                }

        def _download_web(entry):
            filename, url, local_path = entry
            debug.conda_exec("%s -> download %s to %s" % (filename, url, local_path))
            try:
                with requests.get(url, stream=True) as r:
                    with open(local_path, "wb") as f:
                        # TODO: We could check the hash here
                        shutil.copyfileobj(r.raw, f)
            except Exception as e:
                return (filename, url, e)
            return (filename, url, None)

        def _transmute(entry):
            filename, new_format, src_file, dst_file, resulting_url = entry
            debug.conda_exec(
                "%s -> transmute %s to %s" % (filename, src_file, dst_file)
            )
            try:
                args = [
                    "t",
                    "--processes",
                    "1",
                    "--force",
                    "--out-folder",
                    os.path.dirname(dst_file),
                    src_file,
                    new_format,
                ]
                self._call_conda(args, binary="cph")
            except CondaException as e:
                return (filename, dst_file, resulting_url, e)
            return (filename, dst_file, resulting_url, None)

        # Setup package_dirs which is where we look for existing packages.
        if use_package_dirs:
            package_dirs = self._package_dirs
        else:
            package_dirs = [tempdir]

        # We are only adding to package_dirs[0] so we only read that one into known_urls
        with CondaLock(self._package_dir_lock_file(package_dirs[0])):
            url_file = os.path.join(package_dirs[0], "urls.txt")
            if os.path.isfile(url_file):
                with open(url_file, "rb") as f:
                    known_urls.update([l.strip().decode("utf-8") for l in f])

        # Iterate over all the filenames that we want to fetch.
        for filename, base_url, cache_url, file_hash in zip(
            filenames, urls, cache_urls, file_hashes
        ):

            web_format = [f for f in CONDA_FORMATS if base_url.endswith(f)]
            if len(web_format) != 1:
                raise CondaException(
                    "URL '%s' does not end with one of the expected formats (%s)"
                    % (base_url, str(CONDA_FORMATS))
                )
            web_format = web_format[0]
            have_files = {}
            found_dir = False
            # Look for it to exist in any of the package_dirs
            for p in [d for d in package_dirs if os.path.isdir(d)]:
                extract_path = os.path.join(p, filename)
                if not require_format and os.path.isdir(extract_path):
                    debug.conda_exec(
                        "%s -> using existing directory %s" % (filename, extract_path)
                    )
                    results.append(
                        LazyFetchResult(
                            filename=filename,
                            url=base_url,
                            is_dir=True,
                            per_format_desc={
                                ".local": {
                                    "cache_url": None,
                                    "local_path": extract_path,
                                    "fetched": False,
                                    "cached": False,
                                    "transmuted": False,
                                }
                            },
                        )
                    )
                    found_dir = True
                    break
                # At this point, we don't have a directory or we need specific tarballs
                for f in CONDA_FORMATS:
                    if f in have_files:
                        continue
                    else:
                        tentative_path = os.path.join(p, "%s%s" % (filename, f))
                        if os.path.isfile(tentative_path):
                            expected_hash = file_hash.get(f)
                            if (
                                not expected_hash
                                or get_md5_hash(tentative_path) == expected_hash
                            ):
                                have_files[f] = tentative_path
                            else:
                                debug.conda_exec(
                                    "%s -> rejecting %s due to hash mismatch (expected %)"
                                    % (
                                        filename,
                                        tentative_path,
                                        expected_hash,
                                    )
                                )
            if found_dir:
                # We didn't need specific tarballs and we found a directory -- happy clam
                continue

            # This code extracts the most preferred source of filename as well as a list
            # of where we can get a given format
            # The most_preferred_source will be:
            #  - if it exists, a local file (among local files, prefer CONDA_PREFERRED_FORMAT)
            #  - if no local files, if they exist, a cached version
            #    (among cached versions, prefer CONDA_PREFERRED_FORMAT)
            #  - if no cached version, the web download
            most_preferred_source = None
            most_preferred_format = None
            available_formats = {}
            dl_local_path = os.path.join(package_dirs[0], "%s{format}" % filename)
            # Check for local files first
            for f in CONDA_FORMATS:
                local_path = have_files.get(f)
                if local_path:
                    src = ("local", local_path, local_path)
                    if most_preferred_source is None:
                        most_preferred_source = src
                        most_preferred_format = f
                    available_formats[f] = src

            # Check for cache paths next
            for f in CONDA_FORMATS:
                cache_path = cache_url.get(f)
                if cache_path:
                    src = ("cache", cache_path, dl_local_path.format(format=f))
                    if most_preferred_source is None:
                        most_preferred_source = src
                        most_preferred_format = f
                    if f not in available_formats:
                        available_formats[f] = src

            # And finally, fall back on the web
            web_src = (
                "web",
                base_url,
                dl_local_path.format(format=web_format),
            )
            if most_preferred_source is None:
                most_preferred_source = web_src
                most_preferred_format = web_format
            if web_format not in available_formats:
                available_formats[f] = web_src

            debug.conda_exec(
                "%s -> preferred %s @ %s:%s"
                % (
                    filename,
                    most_preferred_format,
                    most_preferred_source[0],
                    most_preferred_source[1],
                )
            )

            per_format_desc = {}
            for f in require_format:
                if f in available_formats:
                    per_format_desc.update(
                        _add_to_downloads_list_and_results(
                            filename, f, *available_formats[f]
                        )
                    )
            # If we didn't fetch anything -- add the most preferred format
            if not per_format_desc:
                per_format_desc.update(
                    _add_to_downloads_list_and_results(
                        filename, most_preferred_format, *most_preferred_source
                    )
                )

            convert_from = next(iter(per_format_desc.values()))
            for f in require_format:
                if f not in per_format_desc:
                    transmutes.append(
                        (
                            filename,
                            f,
                            convert_from["local_path"],
                            dl_local_path.format(format=f),
                            self._make_urlstxt_from_url(
                                base_url, f, is_transmuted=True
                            ),
                        )
                    )
                    per_format_desc.update(
                        {
                            f: {
                                "cache_url": None,
                                "local_path": dl_local_path.format(format=f),
                                "fetched": False,
                                "cached": False,
                                "transmuted": True,
                            }
                        }
                    )

            results.append(
                LazyFetchResult(
                    filename=filename,
                    url=base_url,
                    is_dir=False,
                    per_format_desc=per_format_desc,
                )
            )

        # Done going over all the files
        do_download = web_downloads or cache_downloads
        if do_download:
            start = time.time()
            self._echo(
                "    Downloading %d(web) + %d(cache) packages ..."
                % (len(web_downloads), len(cache_downloads)),
                nl=False,
            )

        # Ensure the packages directory exists at the very least
        if do_download and not os.path.isdir(package_dirs[0]):
            os.makedirs(package_dirs[0])

        pending_exceptions = []
        if web_downloads:
            download_results = Pool().imap_unordered(_download_web, web_downloads)
            for filename, src_url, error in download_results:
                if error is not None:
                    pending_exceptions.append(
                        "Error downloading package for '%s': %s"
                        % (filename, str(error))
                    )
                else:
                    if src_url not in known_urls:
                        url_adds.append(src_url)

        if cache_downloads:
            conda_package_root = get_conda_package_root(self._datastore_type)
            storage = DATASTORES[self._datastore_type](conda_package_root)
            with storage.load_bytes([x[0] for x in cache_downloads]) as load_results:
                for (key, tmpfile, _), (filename, local_file) in zip(
                    load_results, [x[1:] for x in cache_downloads]
                ):
                    if not tmpfile:
                        pending_exceptions.append(
                            "Error downloading package from cache for '%s': not found at %s"
                            % (filename, key)
                        )
                    else:
                        url_to_add = self._make_urlstxt_from_cacheurl(key)
                        if url_to_add not in known_urls:
                            url_adds.append(url_to_add)
                        shutil.move(tmpfile, local_file)

        if do_download:
            self._echo(" done in %d seconds." % int(time.time() - start))
        if not pending_exceptions and transmutes:
            start = time.time()
            self._echo("    Transmuting %d packages ..." % len(transmutes), nl=False)
            transmute_results = Pool().imap_unordered(_transmute, transmutes)
            for filename, dst_file, resulting_url, error in transmute_results:
                if error:
                    pending_exceptions.append(
                        "Error transmuting '%s' to '%s': %s"
                        % (filename, dst_file, error)
                    )
                else:
                    if resulting_url not in known_urls:
                        url_adds.append(resulting_url)
            self._echo(" done in %d seconds." % int(time.time() - start))
        if url_adds:
            # Update the urls file in the packages directory so that Conda knows that the
            # files are there
            debug.conda_exec(
                "Adding the following URLs to %s: %s"
                % (os.path.join(package_dirs[0], "urls.txt"), str(url_adds))
            )
            with CondaLock(self._package_dir_lock_file(package_dirs[0])):
                with open(
                    os.path.join(package_dirs[0], "urls.txt"),
                    mode="a",
                    encoding="utf-8",
                ) as f:
                    f.writelines(["%s\n" % l for l in url_adds])
        if pending_exceptions:
            raise CondaException("\n".join(pending_exceptions))

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
        self._echo("    Installing Conda environment at %s ..." % path, nl=False)
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
        self._echo(" done in %d seconds." % int(time.time() - start), timestamp=False)

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
        cache_urls_per_format = env_desc.get(
            "cache_urls_per_format", [{".conda": None}] * len(env_desc["urls"])
        )

        override_urls = {}
        fetch_results = self.lazy_fetch_packages(
            env_desc["order"],
            env_desc["urls"],
            cache_urls_per_format,
            env_desc["hashes_per_format"],
        )
        for r in fetch_results:
            # If we are using a directory, we didn't download any tarball and we need
            # to make sure we list the proper URL in the @EXPLICIT file so that it
            # can use that one
            if not r.is_dir:
                continue
            with open(
                os.path.join(
                    r.per_format_desc[".local"]["local_path"],
                    "info",
                    "repodata_record.json",
                ),
                mode="r",
                encoding="utf-8",
            ) as f:
                info = json.load(f)
                override_urls[r.url] = (info["url"], info["md5"])
        # At this point, we have all the packages that we need so we should be able to
        # just install directly
        start = time.time()
        self._echo("    Extracting and linking Conda environment ...", nl=False)
        urls = []
        hashes = []

        for base_url, base_format, hash_info, cache_info in zip(
            env_desc["urls"],
            env_desc["url_formats"],
            env_desc["hashes_per_format"],
            cache_urls_per_format,
        ):
            if base_url in override_urls:
                override_url, override_hash = override_urls[base_url]
                urls.append(override_url)
                hashes.append(override_hash)
                continue
            # If not override
            potential_preferred = cache_info.get(CONDA_PREFERRED_FORMAT)
            if potential_preferred:
                cached_candidate = (CONDA_PREFERRED_FORMAT, potential_preferred)
            else:
                cached_candidate = next(iter(cache_info.items()))
            if cached_candidate[1] is not None:
                urls.append(self._make_urlstxt_from_cacheurl(cached_candidate[1]))
                hashes.append(hash_info[cached_candidate[0]])
            else:
                urls.append(base_url)
                hashes.append(hash_info[base_format])
        with tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", delete=False
        ) as explicit_list:
            # We create an explicit file
            lines = ["@EXPLICIT\n"]
            lines.extend(
                ["%s#%s\n" % (url, file_hash) for url, file_hash in zip(urls, hashes)]
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
        self._echo(" done in %s seconds." % int(time.time() - start), timestamp=False)

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
            debug.conda_exec("Conda call: %s" % str([self._bins[binary]] + args))
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
