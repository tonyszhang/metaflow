from collections import defaultdict
from hashlib import md5
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

from metaflow.metaflow_config import CONDA_MAGIC_FILE
from metaflow.metaflow_environment import MetaflowEnvironment
from metaflow.exception import MetaflowException
from metaflow.mflog import BASH_SAVE_LOGS

from .conda_step_decorator import CondaStepDecorator
from .conda import Conda, CondaException
from . import (
    arch_id,
    get_conda_manifest_path,
    get_conda_package_root,
    read_conda_manifest,
    write_to_conda_manifest,
)

try:
    from urlparse import urlparse
except:
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
                }
            else:
                self._requested_envs[env_id]["steps"].append(step)
            self._flow_datastore_type = step_conda_dec.flow_datastore_type

        self._cached_deps = read_conda_manifest(self._local_root, self._flow.name)

        if self._conda is None:
            self._conda = Conda(echo, self._flow_datastore_type)

        need_resolution = [
            env_id
            for env_id in self._requested_envs.keys()
            if env_id not in self._cached_deps
        ]
        if len(need_resolution):
            self._resolve_environments(echo, need_resolution)

        if self._flow_datastore_type in ("s3", "azure"):
            # We may need to update caches
            update_env_ids = []
            for env_id in self._requested_envs.keys():
                # This includes everything we just resolved (clearly does not have
                # cache URLs) as well as anything that may not have had a cache URL
                # (if for example the user first used a local ds before using a S3 one)
                if "cache_urls" not in self._cached_deps[env_id]:
                    update_env_ids.append(env_id)
            if len(update_env_ids):
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
            echo("    Resolving %d environments in flow  ..." % len(env_ids), nl=False)
        else:
            echo(
                "    Resolving %d of %d environments in flows (others are cached)  ..."
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
                for d in deps:
                    s = d.split("#")
                    urls.append(s[0])
                    hashes.append(s[1])
                payload = {
                    "deps": [
                        d.decode("ascii") for d in self._requested_envs[env_id]["deps"]
                    ],
                    "channels": [
                        c.decode("ascii")
                        for c in self._requested_envs[env_id]["channels"]
                    ],
                    "order": [urlparse(u).path.split("/")[-1] for u in urls],
                    "urls": urls,
                    "hashes": hashes,
                }
                self._cached_deps[env_id] = payload
        duration = int(time.time() - start)
        echo("  done in %d seconds." % duration)

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
        cache_to_file = {}
        upload_files = []
        for env_id in env_ids:
            for u, f, h in zip(
                self._cached_deps[env_id]["urls"],
                self._cached_deps[env_id]["order"],
                self._cached_deps[env_id]["hashes"],
            ):
                if u in url_info:
                    if h != url_info[u]["hash"]:
                        raise CondaException("%s specifies two hashes" % u)
                else:
                    url = urlparse(u)
                    cache_path = os.path.join(url.netloc, url.path.lstrip("/"), h, f)
                    url_info[u] = {
                        "file": f,
                        "hash": h,
                        "cache_path": cache_path,
                        "cached": False,
                        "local_path": None,
                        "arch": self._requested_envs[env_id]["arch"],
                    }
                    cache_to_file[cache_path] = u
        # At this point, we check in our backend storage if we have the files we need
        storage_impl = DATASTORES[self._flow_datastore_type]
        storage = storage_impl(get_conda_package_root(self._flow_datastore_type))
        files_exist = storage.is_file(cache_to_file.keys())

        for exist, v in zip(files_exist, cache_to_file.values()):
            url_info[v]["cached"] = exist
            if not exist:
                upload_files.append(v)

        # Download anything we need to upload
        with tempfile.TemporaryDirectory() as download_dir:
            pkgs_per_arch = {}
            for u in upload_files:
                u_info = url_info[u]
                to_update = pkgs_per_arch.setdefault(
                    u_info["arch"],
                    {"filenames": [], "urls": [], "cache_urls": [], "file_hashes": []},
                )
                to_update["filenames"].append(u_info["file"])
                to_update["urls"].append(u)
                to_update["cache_urls"].append(
                    None
                )  # We definitely don't have it cached
                to_update["file_hashes"].append(u_info["hash"])

            for arch, pkgs_info in pkgs_per_arch.items():
                arch_tmpdir = os.mkdir(os.path.join(download_dir, arch))
                results = self._conda.lazy_fetch_packages(
                    pkgs_info["filenames"],
                    pkgs_info["urls"],
                    pkgs_info["cache_urls"],
                    pkgs_info["file_hashes"],
                    require_tarball=True,
                    requested_arch=arch,
                    tempdir=arch_tmpdir,
                )
                for r in results:
                    url_info[r.url]["local_path"] = r.local_path

            # At this point, we can upload all the files we need to the storage
            paths_and_handles = [
                (url_info[u]["cache_path"], open(url_info[u]["local_path"], "rb"))
                for u in upload_files
            ]
            start = time.time()
            echo(
                "    Caching %d packages to %s ..."
                % (len(paths_and_handles), self._flow_datastore_type),
                nl=False,
            )
            storage.save_bytes(paths_and_handles, len_hint=len(paths_and_handles))
            echo("  done in %d seconds." % int(time.time() - start), nl=False)

        # At this point, we can update _cached_deps with the proper information since
        # we now have everything stored
        for env_id in env_ids:
            cache_urls = [
                url_info[k]["cache_path"] for k in self._cached_deps[env_id]["urls"]
            ]
            self._cached_deps[env_id]["cache_urls"] = cache_urls

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
