"""统一日志：请求标识、批次与判定依据。"""

from __future__ import annotations

import logging


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    logger = logging.getLogger("gateway")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter(
                "%(asctime)s %(levelname)s [%(name)s] %(message)s"
            )
        )
        logger.addHandler(handler)
        logger.setLevel(level)
        logger.propagate = False
    return logger
