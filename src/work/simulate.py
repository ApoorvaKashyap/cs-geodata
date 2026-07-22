"""Dummy worker script to simulate workload and delay."""

import secrets
from time import sleep

from loguru import logger


def sim_work() -> None:
    """Simulate a long-running task by sleeping for a random duration."""
    logger.info("Simulating work...")
    sleep(1 + secrets.randbelow(20))
