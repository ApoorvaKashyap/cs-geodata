import secrets
from time import sleep

from loguru import logger


def sim_work() -> None:
    logger.info("Simulating work...")
    sleep(1 + secrets.randbelow(20))
