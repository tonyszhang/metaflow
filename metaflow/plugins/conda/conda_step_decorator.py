import importlib
import json
import os
import platform
import shutil
import sys
import tempfile

from hashlib import sha1
from itertools import chain

from metaflow.datastore import LocalStorage
from metaflow.decorators import StepDecorator
from metaflow.extension_support import EXT_PKG
from metaflow.metadata import MetaDatum
from metaflow.metaflow_config import (
    CONDA_FORCE_LINUX64,
    get_pinned_conda_libs,
)
from metaflow.metaflow_environment import InvalidEnvironmentException
from metaflow.plugins.env_escape import generate_trampolines
from metaflow.unbounded_foreach import UBF_CONTROL
from metaflow.util import get_metaflow_root

from . import arch_id, read_conda_manifest
from .conda import Conda

try:
    unicode
except NameError:
    unicode = str
    basestring = str


class CondaStepDecorator(StepDecorator):
    """
    Specifies the Conda environment for the step.

    Information in this decorator will augment any
    attributes set in the `@conda_base` flow-level decorator. Hence
    you can use `@conda_base` to set common libraries required by all
    steps and use `@conda` to specify step-specific additions.

    Parameters
    ----------
    libraries : Dict
        Libraries to use for this step. The key is the name of the package
        and the value is the version to use (default: `{}`).
    python : string
        Version of Python to use, e.g. '3.7.4'
        (default: None, i.e. the current Python version).
    disabled : bool
        If set to True, disables Conda (default: False).
    """

    name = "conda"
    defaults = {"libraries": {}, "channels": [], "python": None, "disabled": None}

    conda = None
    existing_environments = set()

    def is_enabled(self, ubf_context=None):
        if ubf_context == UBF_CONTROL:
            return False
        return not next(
            x
            for x in [
                self.attributes["disabled"],
                self._base_attributes["disabled"],
                False,
            ]
            if x is not None
        )

    @property
    def channel_deps(self):
        channels = []

        step_channels = self.attributes["channels"]
        base_channels = self._base_attributes["channels"]

        if isinstance(step_channels, list):
            channels.extend(step_channels)
        channels.extend(base_channels)

        return channels

    @property
    def step_deps(self):
        deps = [b"python==%s" % self._python_version().encode()]
        deps.extend(
            b"%s==%s" % (name.encode("ascii"), ver.encode("ascii"))
            for name, ver in self._lib_deps().items()
        )
        return deps

    @property
    def env_id(self):
        # We will hash the channels too but separately to respect the order
        # specified for them
        deps = self.step_deps
        return "metaflow_%s_%s_%s" % (
            self._flow.name,
            self._arch,
            sha1(b" ".join(chain(sorted(deps), self.channel_deps))).hexdigest(),
        )

    @property
    def flow_datastore_type(self):
        return self._flow_datastore_type

    @property
    def requested_architecture(self):
        return self._arch

    def step_init(self, flow, graph, step, decos, environment, flow_datastore, logger):
        if environment.TYPE != "conda":
            raise InvalidEnvironmentException(
                "The *@conda* decorator requires " "--environment=conda"
            )

        self._echo = logger
        self._env = environment
        self._arch = self._architecture(decos)
        self._flow = flow
        self._step_name = step
        self._flow_datastore_type = flow_datastore.TYPE
        self._base_attributes = self._get_base_attributes()

        self._local_root = LocalStorage.get_datastore_root_from_config(self._echo)
        os.environ["PYTHONNOUSERSITE"] = "1"

    def runtime_init(self, flow, graph, package, run_id):
        # Create a symlink to installed version of metaflow to execute user code against
        path_to_metaflow = os.path.join(get_metaflow_root(), "metaflow")
        path_to_info = os.path.join(get_metaflow_root(), "INFO")
        self._metaflow_home = tempfile.mkdtemp(dir="/tmp")
        self._addl_paths = None
        os.symlink(path_to_metaflow, os.path.join(self._metaflow_home, "metaflow"))

        # Symlink the INFO file as well to properly propagate down the Metaflow version
        # if launching on AWS Batch for example
        if os.path.isfile(path_to_info):
            os.symlink(path_to_info, os.path.join(self._metaflow_home, "INFO"))
        else:
            # If there is no "INFO" file, we will actually create one in this new
            # place because we won't be able to properly resolve the EXT_PKG extensions
            # the same way as outside conda (looking at distributions, etc). In a
            # Conda environment, as shown below (where we set self.addl_paths), all
            # EXT_PKG extensions are PYTHONPATH extensions. Instead of re-resolving,
            # we use the resolved information that is written out to the INFO file.
            with open(
                os.path.join(self._metaflow_home, "INFO"), mode="wt", encoding="utf-8"
            ) as f:
                f.write(json.dumps(self._env.get_environment_info()))

        # Do the same for EXT_PKG
        try:
            m = importlib.import_module(EXT_PKG)
        except ImportError:
            # No additional check needed because if we are here, we already checked
            # for other issues when loading at the toplevel
            pass
        else:
            custom_paths = list(set(m.__path__))  # For some reason, at times, unique
            # paths appear multiple times. We simplify
            # to avoid un-necessary links

            if len(custom_paths) == 1:
                # Regular package; we take a quick shortcut here
                os.symlink(
                    custom_paths[0],
                    os.path.join(self._metaflow_home, EXT_PKG),
                )
            else:
                # This is a namespace package, we therefore create a bunch of directories
                # so we can symlink in those separately and we will add those paths
                # to the PYTHONPATH for the interpreter. Note that we don't symlink
                # to the parent of the package because that could end up including
                # more stuff we don't want
                self.addl_paths = []
                for p in custom_paths:
                    temp_dir = tempfile.mkdtemp(dir=self._metaflow_home)
                    os.symlink(p, os.path.join(temp_dir, EXT_PKG))
                    self.addl_paths.append(temp_dir)

        # Also install any environment escape overrides directly here to enable
        # the escape to work even in non metaflow-created subprocesses
        generate_trampolines(self._metaflow_home)

        # Finally, figure out what current environments we have for this flow. We
        # don't have to rebuild any existing ones
        self.conda = Conda(self._echo, self.flow_datastore_type, mode="local")
        self.existing_environments = self.conda.environments(self._flow.name)

    def runtime_step_cli(
        self, cli_args, retry_count, max_user_code_retries, ubf_context
    ):
        no_force = all([x not in cli_args.commands for x in CONDA_FORCE_LINUX64])
        if self.is_enabled(ubf_context) and no_force:
            # Create the environment we are going to use
            my_env_id = self.env_id
            if my_env_id in self.existing_environments:
                self._echo("Using existing Conda environment %s" % my_env_id)
            else:
                # Otherwise, we read the conda file and create the environment locally
                self._echo("Creating Conda environment %s..." % my_env_id)
                env_desc = read_conda_manifest(self._local_root, self._flow.name)[
                    my_env_id
                ]
                self.conda.create(self._step_name, self.env_id, env_desc)

            # Actually set it up.
            python_path = self._metaflow_home
            if self.addl_paths is not None:
                addl_paths = os.pathsep.join(self.addl_paths)
                python_path = os.pathsep.join([addl_paths, python_path])

            cli_args.env["PYTHONPATH"] = python_path
            cli_args.env["_METAFLOW_CONDA_ENV"] = self.env_id
            cli_args.entrypoint[0] = self.conda.python(self.env_id)

    def task_pre_step(
        self,
        step_name,
        task_datastore,
        meta,
        run_id,
        task_id,
        flow,
        graph,
        retry_count,
        max_retries,
        ubf_context,
        inputs,
    ):
        if ubf_context == UBF_CONTROL:
            os.environ["_METAFLOW_CONDA_ENV"] = self.env_id
        if self.is_enabled(ubf_context):
            # Add the Python interpreter's parent to the path. This is to
            # ensure that any non-pythonic dependencies introduced by the conda
            # environment are visible to the user code.
            env_path = os.path.dirname(os.path.realpath(sys.executable))
            if os.environ.get("PATH") is not None:
                env_path = os.pathsep.join([env_path, os.environ["PATH"]])
            os.environ["PATH"] = env_path

            meta.register_metadata(
                run_id,
                step_name,
                task_id,
                [
                    MetaDatum(
                        field="conda_env_id",
                        value=self.env_id,
                        type="conda_env_id",
                        tags=["attempt_id:{0}".format(retry_count)],
                    )
                ],
            )

    def runtime_finished(self, exception):
        shutil.rmtree(self._metaflow_home)

    def _get_base_attributes(self):
        if "conda_base" in self._flow._flow_decorators:
            return self._flow._flow_decorators["conda_base"].attributes
        return self.defaults

    def _python_version(self):
        return next(
            x
            for x in [
                self.attributes["python"],
                self._base_attributes["python"],
                platform.python_version(),
            ]
            if x is not None
        )

    def _lib_deps(self):
        deps = get_pinned_conda_libs(self._python_version(), self._flow_datastore_type)

        base_deps = self._base_attributes["libraries"]
        deps.update(base_deps)
        step_deps = self.attributes["libraries"]
        if isinstance(step_deps, (unicode, basestring)):
            step_deps = step_deps.strip("\"{}'")
            if step_deps:
                step_deps = dict(
                    map(lambda x: x.strip().strip("\"'"), a.split(":"))
                    for a in step_deps.split(",")
                )
        deps.update(step_deps)
        return deps

    def _architecture(self, decos):
        for deco in decos:
            if deco.name in CONDA_FORCE_LINUX64:
                # force conda resolution for linux-64 architectures
                return "linux-64"
        return arch_id()
