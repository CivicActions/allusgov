from __future__ import annotations

import logging
import sys

from loguru import logger
from scrapy.utils.log import configure_logging

from allusgov.config import settings


class InterceptHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        logger.opt(depth=6, exception=record.exc_info).log(
            level,
            record.getMessage(),
        )


def setup_logging() -> None:
    configure_logging(install_root_handler=False)
    log_dir = settings.BASE_PATH.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    for h in list(root_logger.handlers):
        root_logger.removeHandler(h)
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(InterceptHandler())

    logger.remove()
    logger.add(sys.stderr, level="INFO")

    logger.add(
        log_dir / "app.log",
        level="INFO",
        rotation="10 MB",
        retention="14 days",
        enqueue=True,
    )

    logger.add(
        log_dir / "error.log",
        level="ERROR",
        rotation="10 MB",
        retention="30 days",
        backtrace=True,
        diagnose=True,
        enqueue=True,
    )
