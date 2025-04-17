import logging
import sys

def setup_logger(name: str = "product_groups", level=logging.INFO) -> logging.Logger:
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    if not logger.hasHandlers():  # чтобы не дублировалось
        logger.addHandler(handler)

    return logger