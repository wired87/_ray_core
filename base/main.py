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
        self.logs_dir = r"C:\Users\wired\OneDrive\Desktop\BestBrain\tmp\ray\session_*\logs" if OS_NAME == "nt" else "/tmp/ray/session_*/logs"

        self.include_dashboard = OS_NAME != "nt"
        self.local_mode = OS_NAME == "nt"
        self.ip = socket.gethostbyname(socket.gethostname())
        self.ray_port = 6379
        self.http_port = 8001
        self.env_id = env_id
        self.disable = "0" if OS_NAME == "nt" else "1"
        print("RayBase initialized")
    
    
    def main(self):
        self.start_head()
        self.init_ray()
        self.start_serve()
        self.run_serve()
        self.status()
        self.list_tasks()
        self.list_actors()
        self.timeline()

    def init_ray(self):
        #os.environ["RAY_DISABLE_DASHBOARD"] = self.disable

        for _ in range(10):
            try:
                ray.init(
                    ignore_reinit_error=True,
                    local_mode=False,
                    include_dashboard=self.include_dashboard,
                    address=f"auto",
                )
                break
            except Exception as e:
                print("Retrying ray.init()", e)
                time.sleep(1)

        LOGGER.info(f"ray initialized {ray.is_initialized()}")


    def list_actors(self):
        actors = ray.util.list_named_actors(all_namespaces=True)
        print(f"Aktive Remote-Instanzen: {len(actors)}")
        for actor in actors:
            print(actor)  # Zeigt Name oder Handle



    def start_head(self):
        #include_dashboard = "true" if OS_NAME == "nt" else "false"
        subprocess.run(["ray", "start", "--head", f"--port={self.ray_port}"], check=True)

    def stop_ray(self):
        subprocess.run(["ray", "stop", "--force"], check=True)
    def memory(self):
        #ray memory --stats-only
        subprocess.run(["ray", "memory", "--stats-only"], check=True)

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
            route_prefix=f"/"
        )
        print("✅ serve.run() started successfully")

    def stop(self):
        ray.shutdown()
        print("🛑 ray shutdown")


    def status(self):
        subprocess.run(["ray", "status"])
        
        
    def list_tasks(self):
        subprocess.run(["ray", "list", "tasks"])


    def timeline(self):
        ray.timeline(filename="timeline.json")