import asyncio

import ray


from app_utils import FB_DB_ROOT

@ray.remote
class RayRuntimeUtils:
    def __init__(self, host):
        self.host = host
        self.actor_state_infos = {}
        self.ready = False
        self.database = FB_DB_ROOT



    async def set_workers_inactive(self):
        all_workers:dict = ray.get(self.host["utils_worker"].get_all_refs.remote())
        for name, ref in all_workers.items():
            ray.kill(ref)
            print(f"Worker {name} killed")

        # Upsdrt name
        await asyncio.gather(*[
            self.host["db_worker"].iter_upsert.remote(
                attrs={
                    "status": {
                        "state": "INACTIVE",
                        "info": "null"
                    }
                },
                metadata_path=f"{self.database}/metadata/{worker.name}/"
            )
            for name, worker in all_workers.items()
        ])

    def state_upsert(self, state, nid, worker):
        meta = {
            "status": {
                "state": state,
                "info": "null"
            },
            "pid": worker.pid,
            "node_id": worker.node_id,
            "class_name": worker.class_name
        }
        self.host["db_worker"].iter_upsert.remote(
            attrs=meta,
            metadata_path=f"{self.database}/metadata/{nid}/"
        )
        print(f"State {state} for worker {nid} upserted")


