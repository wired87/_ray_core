import os
import re

import ray

from cluster_nodes.server.types import HOST_TYPE


class WorkerUtils:

    def __init__(self, name):
        self.name=name
        self.dir = os.environ.get("LOGGING_DIR")
        self.log_file=f"{self.dir}/{name}.log"

    def _init_logger(self):
        import logging, os
        os.makedirs("worker_logs", exist_ok=True)
        logger = logging.getLogger(self.name)
        logger.setLevel(logging.INFO)

        handler = logging.FileHandler(self.log_file, mode="a")
        formatter = logging.Formatter(
            fmt=f'[{self.name}]',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)

        if not logger.handlers:  # Verhindert Duplikate
            logger.addHandler(handler)

        return logger







@ray.remote
class LoggerWorker:


    def __init__(self, host, run=True):
        self.logging_root = f"logging"
        self.host: HOST_TYPE = host  # include now: head & pixel ref
        self.run=run
        self.dir = os.environ.get("LOGGING_DIR")
        self.struct = {
            "id": {
                "lines": int,
            }
        }
        self.struct_list = []



    def set_run(self, run:bool):
        self.run = run

    def stream_loggs(self):
        all_workers = ray.get(self.host["utils_worker"].call.remote(method_name="id_map"))
        for w in all_workers:
            self.struct[w] = {
                    "lines": 0,
                    "lines_to_send": {}
                }

        while self.run == True:
            for nid in all_workers:
                # read new logs for all workers and
                # save new lines
                log_file = f"{self.dir}/{nid}.log"
                with open(log_file, "r") as f:
                    file_len = sum(1 for _ in f)
                    if file_len > 1:
                        last_lines = self.struct[nid]["lines"]
                        if file_len > last_lines:
                            for i in range(last_lines, file_len):
                                fline=f[i]
                                fb_dest = f"/{nid}/{self.get_logger_id(fline)}"
                                self.struct[nid]["lines_to_send"][fb_dest] = fline

            # upsert
            data = {}
            for nid, struct in self.struct.items():
                data.update({k: v for k, v in self.struct[nid]["lines_to_send"]})
                ray.get(self.host["db_worker"].call.remote(
                    method_name="upsert_data",
                    path=self.logging_root,
                    data=data
                ))

            # reset
            for nid, struct in self.struct.items():
                self.struct[nid]["lines_to_send"] = {}
            data = {}

    def get_logger_id(self, log_line):
        match = re.match(r"\[(.*?)\]", log_line)
        if match:
            asctime = match.group(1)
            print("Zeitstempel:", asctime)

        return match
