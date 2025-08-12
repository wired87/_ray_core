from ray import serve

from _ray_core.base.main import RayAdminBase
from app_utils import ENV_ID


class TestBase(RayAdminBase):



    def __init__(self):
        super().__init__()


    def test_singel_remote(self):
        self.start_head()
        self.init_ray()

        self.start_serve()
        self.run_serve()
        self.host["head"] = serve.get_deployment_handle(ENV_ID, app_name=ENV_ID)

