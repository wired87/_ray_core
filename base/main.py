import os
import socket
import subprocess
import time
from pathlib import Path

import ray
import requests
from ray import serve
from ray.exceptions import RayActorError

from _ray_core.base._ray_utils import RayUtils
from app_utils import HEAD_SERVER_NAME, ENV_ID, FB_DB_ROOT
from cluster_nodes.head import Head
from cluster_nodes.server.types import HOST_TYPE
from fb_core.real_time_database import FirebaseRTDBManager
from utils.file._yaml import load_yaml
from utils.logger import LOGGER
from utils.run_subprocess import exec_cmd

OS_NAME = os.name


class RayAdminBase(RayUtils):

    def __init__(self):
        super().__init__()
        self.include_dashboard = OS_NAME != "nt"
        self.local_mode = OS_NAME == "nt"
        self.ip = socket.gethostbyname(socket.gethostname())
        self.disable = "0" if OS_NAME == "nt" else "1"
        self.host: HOST_TYPE = {}
        print("RayBase initialized")

    def init_ray_process(self, serve=False):
        self.stop_ray()
        self.start_head()
        self.init_ray()

        if serve is True:
            self.init_serve()

        r"""
        try:
            self.session_dir = r"C:\Users\wired\OneDrive\Desktop\Projects\qfs\tmp\ray\session_latest" if os.name == "nt" else "/tmp/ray/session_latest"

            os.remove(self.session_dir)
            os.makedirs(self.session_dir, exist_ok=True)
        except Exception as e:
            print(f"{self.session_dir} already a dir: {e}")
        """

    def print_actor_states(self):
        self.status()
        self.list_tasks()
        self.list_actors(print_actors=True)
        self.timeline()

    def init_ray(self, namespace_name=None):
        # os.environ["RAY_DISABLE_DASHBOARD"] = self.disable
        os.environ["RAY_LOGGING_CONFIG_ENCODING"] = "JSON"
        print("init ray")
        for _ in range(10):
            try:
                ray.init(
                    ignore_reinit_error=True,
                    local_mode=False,
                    include_dashboard=True,
                    address=f"auto",
                    logging_config=ray.LoggingConfig(
                        encoding="JSON",
                        log_level="INFO",
                        additional_log_standard_attrs=['name']
                    ),
                    # _temp_dir=self.session_dir,
                )
                break
            except Exception as e:
                print("Retrying ray.init()", e)
                time.sleep(1)

        LOGGER.info(f"ray initialized {ray.is_initialized()}")

    def start_head(self):
        ray_port = 6379
        _try = 0
        max_tries = 10
        for i in range(max_tries):
            print(f"try {i} to start head")
            try:
                cmd = ["ray", "start", "--head", f"--port={ray_port}", f"--temp-dir={self.ray_assets_dir}"]
                result = exec_cmd(cmd)
                if result is not None:
                    print("Started Head")
                    return
            except Exception as e:
                print(f"error start head: {e}")
                self.stop_ray()
            time.sleep(5)
        print("Head couldn be started")

    def stop_ray(self):
        try:
            print(exec_cmd(["ray", "stop", "--force"]))
            print("Stopped existing ray processes")
        except Exception as e:
            print(f"error stop: {e}")

    def memory(self):
        # ray memory --stats-only
        exec_cmd(["ray", "memory", "--stats-only"])

    def start_serve(
            self,
    ):
        for i in range(10):
            try:
                print(f"[Try {i + 1}] Starting serve.run()")
                self.init_serve()
                print("✅ serve.start() started successfully")
                break
            except RayActorError as e:
                print(f"⚠️ serve.start() failed, retrying...: {e}")
                time.sleep(2)
            except Exception as e:
                print("🔥 Unexpected error in serve.run():", e)
                time.sleep(2)

    def init_serve(self):
        http_port = 8001
        serve.start(
            http_options={"host": "0.0.0.0", "port": http_port},
            detached=True,
            disable_dashboard=os.name == "nt",
        )



    def stop(self):
        ray.shutdown()
        print("🛑 ray shutdown")

    def status(self):
        exec_cmd(["ray", "status"])

    def list_tasks(self):
        exec_cmd(["ray", "list", "tasks"])

    def timeline(self):
        ray.timeline(filename="timeline.json")

    def create_static_docker_env_vars(self):
        return {
            "DOMAIN": "bestbrain.tech",
            "DATASET_ID": "QCOMPS",
            "GCP_ID": os.environ.get("GCP_PROJECT_ID"),
            "FIREBASE_RTDB": os.environ.get("FIREBASE_RTDB"),
        }

