import os

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

