import json
import os
import shutil
import sys

from metaflow.metaflow_config import (
    DATASTORE_LOCAL_DIR,
    CONDA_MAGIC_FILE,
)

from metaflow.cli import echo

from ..env_escape import generate_trampolines, ENV_ESCAPE_PY

from .conda import Conda


def bootstrap_environment(flow_name, step_name, env_id, datastore_type):
    print("Setting up Conda...")
    my_conda = Conda(echo, datastore_type, mode="remote")
    setup_conda_manifest(flow_name)
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)

    # Fetch all packages
    with open(os.path.join(manifest_folder, CONDA_MAGIC_FILE)) as f:
        env = json.load(f)[env_id]

    # Install the environment
    my_conda.create(step_name, env_id, env, do_symlink=True)

    # Setup anything needed by the escape hatch
    if ENV_ESCAPE_PY is not None:
        cwd = os.getcwd()
        generate_trampolines(cwd)
        # print("Environment escape will use %s as the interpreter" % ENV_ESCAPE_PY)
    else:
        pass
        # print("Could not find a environment escape interpreter")


def setup_conda_manifest(flow_name):
    manifest_folder = os.path.join(os.getcwd(), DATASTORE_LOCAL_DIR, flow_name)
    if not os.path.exists(manifest_folder):
        os.makedirs(manifest_folder)
    shutil.move(
        os.path.join(os.getcwd(), CONDA_MAGIC_FILE),
        os.path.join(manifest_folder, CONDA_MAGIC_FILE),
    )


if __name__ == "__main__":
    bootstrap_environment(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
