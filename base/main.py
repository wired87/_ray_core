import os
import socket
import subprocess
import time
import asyncio

import aiohttp
import ray
import requests
import websockets
from ray import serve
from ray.exceptions import RayActorError

from _ray_core.base._ray_utils import RayUtils
from app_utils import HEAD_SERVER_NAME
from cluster_nodes.head import HeadServer
from cluster_nodes.server.types import HOST_TYPE
from utils.logger import LOGGER

OS_NAME = os.name


class RayAdminBase(RayUtils):

    def __init__(self):
        super().__init__()
        self.include_dashboard = OS_NAME != "nt"
        self.local_mode = OS_NAME == "nt"
        self.ip = socket.gethostbyname(socket.gethostname())
        self.ray_port = 6379
        self.http_port = 8001
        self.disable = "0" if OS_NAME == "nt" else "1"

        self.host:HOST_TYPE = {}

        print("RayBase initialized")
    
    
    def main(self):
        self.start_head()
        self.init_ray()

        self.start_serve()
        self.run_serve()
        self.host["head"] = serve.get_deployment_handle(ENV_ID, app_name=ENV_ID)

        self.status()
        self.list_tasks()
        self.list_actors(print_actors=True)
        self.timeline()

    def init_ray(self):
        #os.environ["RAY_DISABLE_DASHBOARD"] = self.disable
        #os.environ["RAY_LOGGING_CONFIG_ENCODING"] = "JSON"
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


    def start_head(self):
        #include_dashboard = "true" if OS_NAME == "nt" else "false"
        subprocess.run(["ray", "start", "--head", f"--port={self.ray_port}", f"--temp-dir={self.ray_assets_dir}"], check=True)

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
                name=HEAD_SERVER_NAME,
                ).bind(),
            name=HEAD_SERVER_NAME,
            route_prefix="/"
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



    def test_connection_sync(self):
        ws_type = "ws"  # or "wss"
        trgt_vm_ws_port = 8001
        trgt_vm_ip = "127.0.0.1"  # or your VM IP
        trgt_vm_endpoint = f"root/{ENV_ID}"  # Replace with your TEST_ENV_ID
        trgt_vm_domain = f"{ws_type}://{trgt_vm_ip}:{trgt_vm_ws_port}/{trgt_vm_endpoint}"

        async def connect():
            _try = 0
            while _try < 30:
                try:
                    ws = await websockets.connect(trgt_vm_domain)
                    if ws is not None:
                        return ws
                except Exception as e:
                    print(f"[RELAY2CLUSTER] Fehler beim Verbindungsaufbau Versuch {_try + 1}: {e}")
                    _try += 1
                    time.sleep(3)

            print("[RELAY2CLUSTER] Maximale Anzahl an Versuchen erreicht. Verbindung fehlgeschlagen.")
            return False

        # Ein Event Loop starten, um die asynchrone Funktion auszuführen
        return asyncio.run(connect())


    def test_post(self):

        response = requests.post(trgt_vm_domain, {"init": "hi"})
        print("response", response)
        print("response.text", response.text)  # Um die Fehlermeldung von FastAPI zu sehen
        print("response.json()", response.json())  # Wenn die Antwort JSON ist

    async def send_post_request(self, url: str, payload: dict):
        """
        Sendet einen asynchronen POST-Request mit einem JSON-Body.
        """
        print("url:", url)
        # Eine Client-Session erstellen, um die Anfrage zu senden
        async with aiohttp.ClientSession() as session:
            try:
                # Den POST-Request senden. Der 'json' Parameter handhabt die Serialisierung
                # des Python-Dicts und setzt den Content-Type Header korrekt.
                async with session.post(url, json=payload) as response:
                    print(f"Status Code: {response.status}")

                    # Den Response-Body als JSON parsen
                    response_json = await response.json()
                    print("Response Body:", response_json)

                    # Bei einem Fehler einen Exception auslösen
                    response.raise_for_status()
                    return response_json

            except aiohttp.ClientError as e:
                print(f"Ein Fehler ist aufgetreten: {e}")
                return None


    def create_static_docker_env_vars(self):
        return {
            "DOMAIN": "bestbrain.tech",
            "DATASET_ID": "QCOMPS",
            "GCP_ID": os.environ.get("GCP_PROJECT_ID"),
            "FIREBASE_RTDB": os.environ.get("FIREBASE_RTDB"),

        }




rb=RayAdminBase()
ws_type = "http"  # or "wss"
trgt_vm_ws_port = 8001
trgt_vm_ip = "127.0.0.1"  # or your VM IP
trgt_vm_endpoint = f"root/{ENV_ID}"  # Replace with your TEST_ENV_ID
trgt_vm_domain = f"{ws_type}://{trgt_vm_ip}:{trgt_vm_ws_port}/{trgt_vm_endpoint}"

status_payload = {  # InboundPayload
            "data": {
                "type": "start",
            },
            "type": "state_change",
        }

auth_payload = {
    "type": "auth",
    "data": {
        "key": ENV_ID,
        "session_id": "xxx",
    }
}



def activate():
    response = requests.post(trgt_vm_domain, json=auth_payload)
    print(f"Auth response: {response}-{response.json()}")
    response = requests.post(trgt_vm_domain, json=status_payload)
    print(f"State Change response: {response}-{response.json()}")


if __name__ == "__main__":
    print("ENV_ID", ENV_ID)
    activate()


# test_db_worker
# activate