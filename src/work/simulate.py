from random import randint
from time import sleep

from loguru import logger


def sim_work() -> None:
    logger.info("Simulating work...")
    sleep(randint(1, 20))
