import os
import ray


class RayUtils:

    def __init__(self):
        self.ray_assets_dir = r"C:\Users\wired\OneDrive\Desktop\Projects\qfs\tmp\ray" if os.name == "nt" else "/tmp/ray/"
        os.makedirs(self.ray_assets_dir, exist_ok=True)
        #os.makedirs(os.path.join(self.ray_assets_dir, "session_latest"), exist_ok=True)


    def _p(self, msg:str, logger=None):
        if logger is not None:
            logger.info(msg)
        else:
            print(msg)

    def list_actors(self, print_actors=False):
        actors = ray.util.list_named_actors(all_namespaces=True)
        if print_actors is True:
            print(f"Aktive Remote-Instanzen: {len(actors)}")
            for actor in actors:
                print(actor)  # Zeigt Name oder Handle
        return actors
