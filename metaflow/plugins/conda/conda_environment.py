from collections import defaultdict
from hashlib import md5
from itertools import chain
import json
import os
import time
import requests
import shutil
import sys
import tarfile

from hashlib import md5
from io import BytesIO
from multiprocessing.dummy import Pool
import tempfile
from metaflow.datastore import DATASTORES

from metaflow.datastore.local_storage import LocalStorage
from metaflow.debug import debug

from metaflow.metaflow_config import CONDA_MAGIC_FILE
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.exception import MetaflowException
from metaflow.mflog import BASH_SAVE_LOGS

from .conda_step_decorator import CondaStepDecorator
from .conda import Conda, CondaException
from . import (
    CONDA_FORMATS,
    arch_id,
    get_conda_manifest_path,
    get_conda_package_root,
    get_md5_hash,
    read_conda_manifest,
    write_to_conda_manifest,
)


from urllib.parse import urlparse


class CondaEnvironment(MetaflowEnvironment):
    TYPE = "conda"
    _filecache = None
    _conda = None

    def __init__(self, flow):
        self._flow = flow
        self._local_root = None
        self._flow_datastore_type = None
        # key: hash of the environment; value: dict containing:
        #  - "id": key
        #  - "steps": steps using this environment
        #  - "arch": architecture of the environment
        #  - "deps": array of requested dependencies
        #  - "channels": additional channels to search
        self._requested_envs = dict()

        self._cached_deps = None

        # A conda environment sits on top of whatever default environment
        # the user has so we get that environment to be able to forward
        # any calls we don't handle specifically to that one.
        from ...plugins import ENVIRONMENTS
        from metaflow.metaflow_config import DEFAULT_ENVIRONMENT

        if DEFAULT_ENVIRONMENT == self.TYPE:
            # If the default environment is Conda itself then fallback on
            # the default 'default environment'
            self.base_env = MetaflowEnvironment(self._flow)
        else:
            self.base_env = [
                e
                for e in ENVIRONMENTS + [MetaflowEnvironment]
                if e.TYPE == DEFAULT_ENVIRONMENT
            ][0](self._flow)

    def init_environment(self, echo):
        # Print a message for now
        echo("Bootstrapping Conda environment... (this could take a few minutes)")
        self._local_root = LocalStorage.get_datastore_root_from_config(echo)

        for step in self._flow:
            # Figure out the environments that we need to resolve for all steps
            # We will resolve all unique environments in parallel
            step_conda_dec = self._get_conda_decorator(step.__name__)
            env_id = step_conda_dec.env_id
            if env_id not in self._requested_envs:
                self._requested_envs[env_id] = {
                    "id": env_id,
                    "steps": [step],
                    "arch": step_conda_dec.requested_architecture,
                    "deps": step_conda_dec.step_deps,
                    "channels": step_conda_dec.channel_deps,
                    "format": [step_conda_dec.package_format],
                }
            else:
                self._requested_envs[env_id]["steps"].append(step)
                if (
                    step_conda_dec.package_format
                    not in self._requested_envs[env_id]["format"]
                ):
                    self._requested_envs[env_id]["format"].append(
                        step_conda_dec.package_format
                    )
            self._flow_datastore_type = step_conda_dec.flow_datastore_type

        self._cached_deps = read_conda_manifest(self._local_root, self._flow.name)

        if self._conda is None:
            self._conda = Conda(echo, self._flow_datastore_type)

        debug.conda_exec(
            "Loaded cached environments for %s" % str(self._cached_deps.keys())
        )
        need_resolution = [
            env_id
            for env_id in self._requested_envs.keys()
            if env_id not in self._cached_deps
        ]
        if debug.conda:
            debug.conda_exec("Resolving environments:")
            for env_id, info in self._requested_envs.items():
                debug.conda_exec("%s: %s" % (env_id, str(info)))
        if len(need_resolution):
            self._resolve_environments(echo, need_resolution)

        if self._flow_datastore_type in ("s3", "azure"):
            # We may need to update caches
            update_env_ids = []
            for env_id, request in self._requested_envs.items():
                # This includes everything we just resolved (clearly does not have
                # cache URLs) as well as anything that may not have had a cache URL
                # (if for example the user first used a local ds before using a S3 one)
                # as well as anything for which we don't have the right package format
                do_update = False
                if "cache_urls_per_format" not in self._cached_deps[env_id]:
                    do_update = True
                else:
                    req_formats = set(request["format"])
                    cached_urls = self._cached_deps[env_id]["cache_urls_per_format"]
                    do_update = not all(
                        [req_formats.issubset(set(x.keys())) for x in cached_urls]
                    )
                if do_update:
                    update_env_ids.append(env_id)
            debug.conda_exec("Caching environments %s" % str(update_env_ids))
            if update_env_ids:
                self._cache_environments(echo, update_env_ids)
        else:
            update_env_ids = need_resolution

        # We can write the information back out. The environment will be created
        # locally if needed by runtime_init in the Conda step decorator or remotely
        # in remote_bootstrap.py.
        write_to_conda_manifest(
            self._local_root,
            self._flow.name,
            {k: v for k, v in self._cached_deps.items() if k in update_env_ids},
        )

        # Delegate to whatever the base environment needs to do.
        self.base_env.init_environment(echo)

    def validate_environment(self, echo):
        return self.base_env.validate_environment(echo)

    def decospecs(self):
        # Apply conda decorator and base environment's decorators to all steps
        return ("conda",) + self.base_env.decospecs()

    def _resolve_environments(self, echo, env_ids):
        start = time.time()
        if len(env_ids) == len(self._requested_envs):
            echo("    Resolving %d environments in flow ..." % len(env_ids), nl=False)
        else:
            echo(
                "    Resolving %d of %d environments in flows (others are cached) ..."
                % (len(env_ids), len(self._requested_envs)),
                nl=False,
            )

        def _resolve(env_desc):
            return (
                env_desc["id"],
                self._conda.resolve(
                    env_desc["steps"],
                    env_desc["id"],
                    env_desc["deps"],
                    env_desc["channels"],
                    env_desc["arch"],
                ),
            )

        if len(env_ids):
            explicit_deps = Pool().imap_unordered(
                _resolve, [v for k, v in self._requested_envs.items() if k in env_ids]
            )
            for env_id, deps in explicit_deps:
                urls = []
                hashes = []
                filenames = []
                url_formats = []
                for d in deps:
                    s = d.split("#")
                    urls.append(s[0])

                    filename = os.path.split(urlparse(s[0]).path)[1]
                    for f in CONDA_FORMATS:
                        if filename.endswith(f):
                            hashes.append({f: s[1]})
                            filenames.append(filename[: -len(f)])
                            url_formats.append(f)
                            break
                    else:
                        raise CondaException(
                            "URL '%s' is not a supported format (%s)"
                            % (s[0], CONDA_FORMATS)
                        )

                payload = {
                    "deps": [
                        d.decode("ascii") for d in self._requested_envs[env_id]["deps"]
                    ],
                    "channels": [
                        c.decode("ascii")
                        for c in self._requested_envs[env_id]["channels"]
                    ],
                    "order": filenames,
                    "url_formats": url_formats,
                    "urls": urls,
                    "hashes_per_format": hashes,
                }
                debug.conda_exec(
                    "For environment %s (deps: %s), need packages %s"
                    % (env_id, str(payload["deps"]), str(payload["order"]))
                )
                self._cached_deps[env_id] = payload
        duration = int(time.time() - start)
        echo(" done in %d seconds." % duration)

    def _cache_environments(self, echo, env_ids):
        # The logic behind this function is as follows:
        #  - check in the S3/Azure storage to see if the file exists
        #  - if it does, we are all good and we update the cache_urls
        #  - if it does not, check if the file is locally installed (if same arch)
        #    + if installed, check if it matches the MD5 hash and if all checks out, use to upload
        #    + if not, download it
        #  - at this point, we have the tarballs so upload to S3/Azure

        # key: URL; value: {"hash" , "cache_path", "cached", "arch", "local_path"}
        url_info = {}
        cache_paths_to_check_keys = []
        cache_paths_to_check = []
        upload_files = []
        for env_id in env_ids:
            requested_formats = self._requested_envs[env_id]["format"]
            for (
                base_url,
                url_format,
                filename,
                hash_per_format,
                cache_path_per_format,
            ) in zip(
                self._cached_deps[env_id]["urls"],
                self._cached_deps[env_id]["url_formats"],
                self._cached_deps[env_id]["order"],
                self._cached_deps[env_id]["hashes_per_format"],
                self._cached_deps[env_id].get(
                    "cache_urls_per_format",
                    [{}] * len(self._cached_deps[env_id]["order"]),
                ),
            ):
                if base_url in url_info:
                    # Update based on possibly different formats. It is possible
                    # that different environments have slightly different information
                    cur_record = url_info[base_url]
                    for pkg_format, filehash in hash_per_format.items():
                        if pkg_format in cur_record["hash_per_format"]:
                            if cur_record["hash_per_format"][pkg_format] != filehash:
                                raise CondaException(
                                    "%s specifies two hashes for format %s"
                                    % (base_url, pkg_format)
                                )
                        else:
                            cur_record["hash_per_format"].update({pkg_format: filehash})
                            cached_value = cache_path_per_format.get(pkg_format)
                            if cached_value:
                                cur_record["cache_path_per_format"].update(
                                    {pkg_format: cached_value}
                                )
                                cur_record["cached_per_format"].update(
                                    {pkg_format: True}
                                )
                else:
                    # We always add the url_format to the requested format because
                    # this allows us to check for a cached version of it if we need
                    # another format.
                    my_requested_formats = list(requested_formats)
                    if url_format not in my_requested_formats:
                        my_requested_formats.append(url_format)
                    url_info[base_url] = {
                        "file": filename,
                        "url_format": url_format,
                        "hash_per_format": dict(hash_per_format),
                        "cache_path_per_format": dict(cache_path_per_format),
                        "cached_per_format": {
                            k: (k in cache_path_per_format)
                            for k in my_requested_formats
                        },
                        "local_path_per_format": {},
                        "arch": self._requested_envs[env_id]["arch"],
                    }

        # We will now check various locations in the datastore to see if we have
        # the files in cache.
        for base_url, desc in url_info.items():
            if not all(desc["cached_per_format"].values()):
                # We need to go check something. We check the "base" path (which is
                # the one using the base url)
                # We also check the special link files for other formats we are
                # interested in
                base_cache_url = Conda.make_cache_url(
                    base_url, desc["hash_per_format"][desc["url_format"]]
                )
                for req_format, is_cached in desc["cached_per_format"].items():
                    if is_cached:
                        continue
                    if req_format == desc["url_format"]:
                        cache_paths_to_check.append(base_cache_url)
                        cache_paths_to_check_keys.append((base_url, req_format))
                        debug.conda_exec(
                            "%s -> check cache @ %s" % (base_url, base_cache_url)
                        )
                    else:
                        cache_path = os.path.join(
                            os.path.split(base_cache_url)[0], "%s.lnk" % req_format
                        )
                        cache_paths_to_check.append(cache_path)
                        debug.conda_exec(
                            "%s:%s -> check link @ %s"
                            % (base_url, req_format, cache_path)
                        )
                        cache_paths_to_check_keys.append((base_url, req_format))

        # At this point, we check in our backend storage if we have the files we need
        storage_impl = DATASTORES[self._flow_datastore_type]
        storage = storage_impl(get_conda_package_root(self._flow_datastore_type))
        files_exist = storage.is_file(cache_paths_to_check)

        link_files_to_get = []
        link_files_to_get_keys = []
        for exist, req_url, (base_url, pkg_format) in zip(
            files_exist, cache_paths_to_check, cache_paths_to_check_keys
        ):
            u_info = url_info[base_url]
            if exist:
                u_info["cached_per_format"][pkg_format] = True
                debug.conda_exec("%s -> Found cache file %s" % (base_url, req_url))
                if pkg_format != u_info["url_format"]:
                    # This means that we have a .lnk file, we need to actually fetch
                    # it and determine where it points to so we can update the cache URLs
                    # and hashes
                    debug.conda_exec(
                        "%s:%s -> Found link file" % (base_url, pkg_format)
                    )
                    link_files_to_get.append(req_url)
                    link_files_to_get_keys.append((base_url, pkg_format))
                else:
                    u_info["cache_path_per_format"][pkg_format] = req_url
                    u_info["hash_per_format"][pkg_format] = Conda.parse_cache_url(
                        req_url
                    ).hash
            else:
                upload_files.append((base_url, pkg_format))

        # Get the link files to properly update the cache URLs and hashes
        if link_files_to_get:
            with storage.load_bytes(link_files_to_get) as loaded:
                for (_, tmpfile, _), (base_url, pkg_format) in zip(
                    loaded, link_files_to_get_keys
                ):
                    u_info = url_info[base_url]
                    with open(tmpfile, mode="r", encoding="utf-8") as f:
                        cached_hash, cached_path = f.read().strip().split(" ")
                        debug.conda_exec(
                            "%s:%s -> File at %s (hash %s)"
                            % (base_url, pkg_format, cached_path, cached_hash)
                        )
                        u_info["cache_path_per_format"][pkg_format] = cached_path
                        u_info["hash_per_format"][pkg_format] = cached_hash

        # Download anything we need to upload
        # To simplify for now, we actually request all files in the maximum format required.
        # This is not entirely accurate (it is a superset) but it simplifies the code
        required_format = set()
        with tempfile.TemporaryDirectory() as download_dir:
            pkgs_per_arch = {}
            for base_url, req_format in upload_files:
                required_format.add(req_format)
                u_info = url_info[base_url]
                to_update = pkgs_per_arch.setdefault(
                    u_info["arch"],
                    {"filenames": [], "urls": [], "cache_urls": [], "file_hashes": []},
                )
                to_update["filenames"].append(u_info["file"])
                to_update["urls"].append(base_url)
                to_update["cache_urls"].append(u_info["cache_path_per_format"])
                to_update["file_hashes"].append(u_info["hash_per_format"])

            upload_files = []  # Reset as we may add more stuff like link files and
            # what not
            for arch, pkgs_info in pkgs_per_arch.items():
                arch_tmpdir = os.mkdir(os.path.join(download_dir, arch))
                results = self._conda.lazy_fetch_packages(
                    pkgs_info["filenames"],
                    pkgs_info["urls"],
                    pkgs_info["cache_urls"],
                    pkgs_info["file_hashes"],
                    require_format=list(required_format),
                    requested_arch=arch,
                    tempdir=arch_tmpdir,
                )
                for r in results:
                    u_info = url_info[r.url]
                    for pkg_format, pkg_desc in r.per_format_desc.items():
                        # NOTA: When transmuting, we may actually upload more here
                        # because we may have gotten things that were not needed.
                        known_hash = u_info["hash_per_format"].get(pkg_format)
                        if known_hash is None:
                            known_hash = get_md5_hash(pkg_desc["local_path"])
                            u_info["hash_per_format"][pkg_format] = known_hash

                        cache_path = pkg_desc["cache_url"]
                        if cache_path is None:
                            cache_path = Conda.make_cache_url(
                                r.url,
                                known_hash,
                                file_format=pkg_format,
                                is_transmuted=pkg_desc["transmuted"],
                            )
                        u_info["cache_path_per_format"][pkg_format] = cache_path

                        if not pkg_desc["cached"]:
                            upload_files.append((cache_path, pkg_desc["local_path"]))
                            debug.conda_exec(
                                "%s -> will upload %s to %s"
                                % (r.url, pkg_desc["local_path"], cache_path)
                            )
                        if (
                            not pkg_desc["cached"]
                            and pkg_format != u_info["url_format"]
                        ):
                            # We upload a special .lnk file to the main directory
                            # so we can find it later
                            base_cache_url = Conda.make_cache_url(
                                r.url, u_info["hash_per_format"][u_info["url_format"]]
                            )
                            lnk_url = os.path.join(
                                os.path.split(base_cache_url)[0], "%s.lnk" % pkg_format
                            )
                            with tempfile.NamedTemporaryFile(
                                delete=False, mode="w", encoding="utf-8"
                            ) as lnk_file:
                                debug.conda_exec(
                                    "%s:%s -> will upload link @@%s %s@@ to %s"
                                    % (
                                        r.url,
                                        pkg_format,
                                        known_hash,
                                        cache_path,
                                        lnk_url,
                                    )
                                )
                                lnk_file.write("%s %s" % (known_hash, cache_path))
                                upload_files.append((lnk_url, lnk_file.name))

            # At this point, we can upload all the files we need to the storage
            def paths_and_handles():
                for cache_path, local_path in upload_files:
                    with open(local_path, mode="rb") as f:
                        yield cache_path, f

            if upload_files:
                start = time.time()
                echo(
                    "    Caching %d packages to %s ..."
                    % (len(upload_files), self._flow_datastore_type),
                    nl=False,
                )
                storage.save_bytes(paths_and_handles(), len_hint=len(upload_files))
                echo(" done in %d seconds." % int(time.time() - start), nl=False)
            else:
                echo("    All packages cached in %s." % self._flow_datastore_type)

        # At this point, we can update _cached_deps with the proper information since
        # we now have everything stored
        for env_id in env_ids:
            new_hashes = [
                url_info[k]["hash_per_format"]
                for k in self._cached_deps[env_id]["urls"]
            ]
            new_cached_urls = [
                url_info[k]["cache_path_per_format"]
                for k in self._cached_deps[env_id]["urls"]
            ]
            self._cached_deps[env_id]["hashes_per_format"] = new_hashes
            self._cached_deps[env_id]["cache_urls_per_format"] = new_cached_urls

    def _get_conda_decorator(self, step_name):
        step = next(step for step in self._flow if step.name == step_name)
        decorator = next(
            deco for deco in step.decorators if isinstance(deco, CondaStepDecorator)
        )
        # Guaranteed to have a conda decorator because of self.decospecs()
        return decorator

    def _get_env_id(self, step_name):
        conda_decorator = self._get_conda_decorator(step_name)
        if conda_decorator.is_enabled():
            return conda_decorator.env_id
        return None

    def _get_executable(self, step_name):
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            # The create method in Conda() sets up this symlink when creating the
            # environment.
            return os.path.join(".", "__conda_python")
        return None

    def bootstrap_commands(self, step_name, datastore_type):
        # Bootstrap conda and execution environment for step
        env_id = self._get_env_id(step_name)
        if env_id is not None:
            return [
                "export CONDA_START=$(date +%s)",
                "echo 'Bootstrapping environment ...'",
                'python -m metaflow.plugins.conda.remote_bootstrap "%s" "%s" %s "%s"'
                % (self._flow.name, step_name, env_id, datastore_type),
                "echo 'Environment bootstrapped.'",
                "export CONDA_END=$(date +%s)",
            ]
        return []

    def add_to_package(self):
        files = self.base_env.add_to_package()
        # Add conda manifest file to job package at the top level.
        path = get_conda_manifest_path(self._local_root, self._flow.name)
        if os.path.exists(path):
            files.append((path, os.path.basename(path)))
        return files

    def pylint_config(self):
        config = self.base_env.pylint_config()
        # Disable (import-error) in pylint
        config.append("--disable=F0401")
        return config

    def executable(self, step_name):
        # Get relevant python interpreter for step
        executable = self._get_executable(step_name)
        if executable is not None:
            return executable
        return self.base_env.executable(step_name)

    @classmethod
    def get_client_info(cls, flow_name, metadata):
        if cls._filecache is None:
            from metaflow.client.filecache import FileCache

            cls._filecache = FileCache()
        info = metadata.get("code-package")
        env_id = metadata.get("conda_env_id")
        if info is None or env_id is None:
            return {"type": "conda"}
        info = json.loads(info)
        _, blobdata = cls._filecache.get_data(
            info["ds_type"], flow_name, info["location"], info["sha"]
        )
        with tarfile.open(fileobj=BytesIO(blobdata), mode="r:gz") as tar:
            conda_file = tar.extractfile(CONDA_MAGIC_FILE)
        if conda_file is None:
            return {"type": "conda"}
        info = json.loads(conda_file.read().decode("utf-8"))
        new_info = {
            "type": "conda",
            "explicit": info[env_id]["explicit"],
            "deps": info[env_id]["deps"],
        }
        return new_info

    def get_package_commands(self, code_package_url, datastore_type):
        return self.base_env.get_package_commands(code_package_url, datastore_type)

    def get_environment_info(self):
        return self.base_env.get_environment_info()
