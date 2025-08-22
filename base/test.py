from ray import serve

from _ray_core.base.main import RayAdminBase


class TestBase(RayAdminBase):



    def __init__(self):
        super().__init__()


    def test_singel_remote(self):
        namespace = None
        self.start_head()
        self.init_ray()

        self.start_serve(namespace)
        self.create_head_server()
        self.host["HEAD"] = serve.get_deployment_handle(ENV_ID, app_name=ENV_ID)

