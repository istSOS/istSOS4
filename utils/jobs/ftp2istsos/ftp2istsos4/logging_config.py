import logging
import re
import sys
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path


class TeeStream:
    def __init__(self, stream, logger, level):
        self.stream = stream
        self.logger = logger
        self.level = level
        self.buffer = ""

    def write(self, data):
        if not data:
            return 0

        self.stream.write(data)
        self.buffer += data
        while "\n" in self.buffer:
            line, self.buffer = self.buffer.split("\n", 1)
            if line:
                self.logger.log(self.level, line)
        return len(data)

    def flush(self):
        self.stream.flush()
        if self.buffer:
            self.logger.log(self.level, self.buffer)
            self.buffer = ""


def log_level(config):
    log_level_name = str(config.get("log_level") or "INFO").upper()
    return getattr(logging, log_level_name, logging.INFO)


def log_rotation(config):
    max_bytes = int(config.get("log_max_bytes") or 5 * 1024 * 1024)
    backup_count = int(config.get("log_backup_count") or 5)
    return max_bytes, backup_count


def log_formatter():
    return logging.Formatter(
        "%(asctime)s %(levelname)-5s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def rotating_file_handler(config, log_file):
    max_bytes, backup_count = log_rotation(config)
    log_file.parent.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        log_file,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setFormatter(log_formatter())
    return handler


def configure_logging(config):
    log_file = Path(config.get("log_file") or "logs/ftp2istsos4.log")
    handler = rotating_file_handler(config, log_file)

    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    root_logger.setLevel(log_level(config))
    root_logger.addHandler(handler)

    output_logger = logging.getLogger("ftp2istsos4.output")
    sys.stdout = TeeStream(sys.stdout, output_logger, logging.INFO)
    sys.stderr = TeeStream(sys.stderr, output_logger, logging.ERROR)

    print(f"Logging to {log_file}", flush=True)


def slugify_log_name(value):
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-") or "source"


def source_log_path(config, index, label):
    log_dir = Path(config.get("source_log_dir") or "logs/sources")
    slug = slugify_log_name(re.sub(r"^\[\d+\]\s*", "", label))
    return log_dir / f"{index:02d}-{slug}.log"


@contextmanager
def source_log_context(config, index, label):
    log_file = source_log_path(config, index, label)
    handler = rotating_file_handler(config, log_file)
    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    try:
        print(f"Source log: {log_file}", flush=True)
        yield log_file
    finally:
        root_logger.removeHandler(handler)
        handler.close()
