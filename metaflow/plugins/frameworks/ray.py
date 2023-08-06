import inspect
import subprocess
import pickle
import tempfile
import os
import sys
from metaflow import current
from metaflow.plugins.parallel_decorator import ParallelDecorator
import time

class RayParallelDecorator(ParallelDecorator):

    name = "ray_parallel"
    defaults = {"master_port": None}
    IS_PARALLEL = True

    def setup_distributed_env(self, flow):
        self._setup_ray_distributed()

    def _setup_ray_distributed(self):

        # Choose port depending on run id to reduce probability of collisions, unless
        # provided by the user. 
        subprocess.Popen(
            [sys.executable, "-m", "pip", "install", "-U", "ray[air]==2.5.0", "pydantic==1.10.12"]
        ).wait()

        try:
            self.master_port = self.attributes["master_port"] or (6379 + abs(int(current.run_id)) % 1000)
        except:
            # if `int()` fails, i.e. `run_id` is not an `int`, use just a constant port. Can't use `hash()`,
            # as that is not constant.
            self.master_port = 6379

        if current.parallel.node_index == 0:
            print(f"The Master Node IP address is: {current.parallel.main_ip}")
            subprocess.Popen(f"RAY_BACKEND_LOG_LEVEL=debug ray start --head --node-ip-address {current.parallel.main_ip} --port {self.master_port}", shell=True).wait()
        else:
            import ray
            node_ip_address = ray._private.services.get_node_ip_address()
            print(f"The Master Node IP address is: {current.parallel.main_ip}")
            print(f"The Node IP address is: {node_ip_address}")
            subprocess.Popen(f"RAY_BACKEND_LOG_LEVEL=debug ray start --node-ip-address {node_ip_address} --address {current.parallel.main_ip}:{self.master_port}", shell=True).wait()

        time.sleep(5*int(current.parallel.num_nodes))

        if current.parallel.node_index == 0:
            import ray
            ray.init()