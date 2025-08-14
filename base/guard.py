import logging
import os

from fastapi import Body
from ray import serve

from _ray_core.base._ray_utils import RayUtils
from _ray_core.base.main import RayAdminBase
from app_utils import APP
from utils.id_gen import generate_id


@serve.deployment(
    num_replicas=1,
    ray_actor_options={"num_cpus": .4},
    max_ongoing_requests=10
)
@serve.ingress(APP)
class Guard:
    """
    Guard of a docker container.
    Handles creation and storage of new anmespaces

    """
    def __init__(self):
        self.logger = logging.getLogger("ray.serve")
        self.logger.info("Initializing HeadDeployment...")
        self.ray_admin = RayAdminBase()
        self.namespace = {
            "example_id": {
                "object_store": None,
                "namespace_ref": None
            }
        }

    @APP.post("/root/guard")
    async def post(self, payload: dict = Body()):
        self.logger.info(f"Guard: post message registered:{payload}")
        try:
            response = await self.handle_extern_message(payload)
            return {"status": "success", "data": response}
        except Exception as e:
            self.logger.error(f"Error while processing: {e}")
            data = e
        return {"status": "error", "data": data}



    async def handle_extern_message(self, payload):
        """
        Entry for all incoming & validated ws (or local) messages
        """
        try:
            self.logger.info(f"MSG FROM EXTERM RECEIVED: {payload}")
            payload_type = payload["type"]
            data = payload.get("data")
            if payload_type == "auth" or (self.session_id is None or self.local_key is None):
                return self._init_hs_relay(data)

            elif payload_type == "deploy":
                self.logger.info("Statechange detected")
                # create cluster in new namespace
                self.ray_admin.main(data)

                return {"status": "success", "msg": f"Applied {state} to workers", "type": "status_success_distribution"}
            else:
                self.logger.info(f"invalid request {payload_type}")
                return {"response": "Handshake pong"}
        except Exception as e:
            self.logger.error(f"Error in SERVE: {e}")

    def _init_hs_relay(self, data):
        """
        :param msg: key, sesion_id
        """
        self.logger.info("Guard: Init request received")
        key = None
        if "session_id" in data and "key" in data:  # and "env_vars" in data

            key = data["key"]
            session_id = data.get("session_id")
            if session_id is not None:
                # Overwrite session_id
                self.session_id = data["session_id"]

                # distribute to db_worker
                os.environ["SESSION_ID"] = self.session_id

                # Build sys from given params
                self.build_env(data)

            self.local_key = generate_id()

            # todo distribute session_id to db
        return dict(
            response_key=self.local_key,
            session_id=self.session_id,
            key=key,
            actor_info=self.get_actor_info()
        )