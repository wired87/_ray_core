import os
import socket
import subprocess
import time

import ray
import requests
from ray import serve
from ray.exceptions import RayActorError

from _ray_core.base._ray_utils import RayUtils
from app_utils import SESSION_ID, HEAD_SERVER_NAME
from cluster_nodes.head import Head
from cluster_nodes.server.types import HOST_TYPE
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
        self.host:HOST_TYPE = {}


        print("RayBase initialized")

    def init_ray_process(self, serve=False):
        self.stop_ray()
        self.start_head()
        self.init_ray()

        if serve is True:
            self.init_serve()

    def print_actor_states(self):
        self.status()
        self.list_tasks()
        self.list_actors(print_actors=True)
        self.timeline()

    def init_ray(self, namespace_name=None):
        #os.environ["RAY_DISABLE_DASHBOARD"] = self.disable
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
                )
                break
            except Exception as e:
                print("Retrying ray.init()", e)
                time.sleep(1)

        LOGGER.info(f"ray initialized {ray.is_initialized()}")



    def start_head(self):
        ray_port = 6379
        cmd = ["ray", "start", "--head", f"--port={ray_port}", f"--temp-dir={self.ray_assets_dir}"]
        exec_cmd(cmd)
        print("Started Head")

    def stop_ray(self):
        try:
            exec_cmd(["ray", "stop", "--force"])
        except Exception as e:
            print(f"error stop: {e}")

    def memory(self):
        #ray memory --stats-only
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


    def create_head_server(
            self,
            name,
            attrs
    ):
        """serve.run(
            Head.options(
                name=HEAD_SERVER_NAME,
                ).bind(),
            name=HEAD_SERVER_NAME,
            route_prefix=f"/{SESSION_ID.replace('_','-')}"
        )"""
        ref = Head.options(
            name=HEAD_SERVER_NAME,
            lifetime="detached",
        ).remote()

        #ref = serve.get_deployment_handle(HEAD_SERVER_NAME, app_name=HEAD_SERVER_NAME)
        self.host["HEAD"] = ref
        print("✅ Head started successfully")
        return ref

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

req_type = "http"  # or "wss"
trgt_vm_ws_port = 8001
trgt_vm_ip = "127.0.0.1"  # or your VM IP
trgt_vm_endpoint = f"root/"  # Replace with your TEST_ENV_ID
trgt_vm_domain = f"{req_type}://{trgt_vm_ip}:{trgt_vm_ws_port}/{trgt_vm_endpoint}"

vars_dict = {
    "DOMAIN": os.environ.get("DOMAIN"),
    "USER_ID": os.environ.get("USER_ID"),
    "GCP_ID": os.environ.get("GCP_ID"),
    "ENV_ID": os.environ.get("ENV_ID"),
    "INSTANCE": os.environ.get("FIREBASE_RTDB"),
    "STIM_STRENGTH": os.environ.get("STIM_STRENGTH"),
}

auth_payload = {
    "type": "auth",
    "data": {
        "key": SESSION_ID
    }
}

deploy_payload = {
    "type": "deploy",
    "data": {
        "stim_cfg": {}
    }
}

state_payload = {  # InboundPayload
    "data": {
        "type": "start",
    },
    "type": "state_change",
}

def activate():
    print(f"Requesting trgt: {trgt_vm_domain}")
    response = requests.post(trgt_vm_domain, json=auth_payload)
    print(f"Auth response: {response}-{response.json()}")
    response = requests.post(trgt_vm_domain, json=state_payload)
    print(f"State Change response: {response}-{response.json()}")


if __name__ == "__main__":
    activate()
