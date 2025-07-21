import os
import socket
import subprocess
import time

import ray
from ray import serve
from ray.exceptions import RayActorError

from cluster_nodes.head import HeadServer
from utils.logger import LOGGER

OS_NAME = os.name

class RayAdminBase:

    def __init__(self, env_id):
        self.logs_dir = r"C:\Users\wired\OneDrive\Desktop\BestBrain\tmp\_ray_core\session_*\logs" if OS_NAME == "nt" else "/tmp/_ray_core/session_*/logs"
        os.makedirs(self.logs_dir, exist_ok=True)
        os.environ["TMPDIR"] = "/tmp/_ray_core"

        self.include_dashboard = OS_NAME != "nt"
        self.local_mode = OS_NAME == "nt"
        self.ip = socket.gethostbyname(socket.gethostname())
        self.ray_port = 6379
        self.http_port = 8001
        self.env_id = env_id
        self.disable = "0" if OS_NAME == "nt" else "1"
        print("RayBase initialized")

    def init_ray(self):
        os.environ["RAY_DISABLE_DASHBOARD"] = self.disable

        for _ in range(10):
            try:
                ray.init(
                    ignore_reinit_error=True,
                    local_mode=self.local_mode,
                    include_dashboard=self.include_dashboard,
                    address=f"auto",
                )
                break
            except Exception as e:
                print("Retrying ray.init()", e)
                time.sleep(1)

        LOGGER.info(f"ray initialized {ray.is_initialized()}")

    def start_head(self):
        include_dashboard = "true" if OS_NAME == "nt" else "false"
        subprocess.run(["ray", "start", "--head", f"--port={self.ray_port}", f"--include-dashboard={include_dashboard}"], check=True)

    def stop_ray(self):
        subprocess.run(["ray", "stop", "--force"], check=True)

    def start_serve(self):
        for i in range(10):
            try:
                print(f"[Try {i + 1}] Starting serve.run()")
                serve.start(
                    http_options={"host": "0.0.0.0", "port": self.http_port},
                    detached=True,
                    disable_dashboard=os.name == "nt"
                )
                print("✅ serve.start() started successfully")
                break
            except RayActorError as e:
                print(f"⚠️ serve.start() failed, retrying...: {e}")
                time.sleep(2)
            except Exception as e:
                print("🔥 Unexpected error in serve.run():", e)
                time.sleep(2)

    def run_serve(self):
        serve.run(
            HeadServer.options(
                name=self.env_id
            ).bind(),
            route_prefix=f"/{self.env_id}"
        )
        print("✅ serve.run() started successfully")

    def stop(self):
        ray.shutdown()
        print("🛑 ray shutdown")


    def status(self):
        subprocess.run(["ray", "status"])