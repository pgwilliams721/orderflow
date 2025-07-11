import logging
import sys
from pythonjsonlogger import jsonlogger
from logging.handlers import RotatingFileHandler
from pathlib import Path

DEFAULT_LOG_FILE_NAME = "delta_rollover_bot.log"
DEFAULT_LOG_DIR = Path("./logs") # Store logs in a dedicated directory

def setup_logging(config=None):
    """
    Configures logging with both console and JSON file output.
    Uses config for log level and log file path if provided.
    """
    log_level_str = "INFO"
    log_file_path_str = None

    if config:
        log_level_str = config.get("logging.log_level", "INFO").upper()
        log_file_path_str = config.get("logging.log_file", DEFAULT_LOG_FILE_NAME)

    if not log_file_path_str: # Fallback if config doesn't provide it or config is None
        log_file_path_str = DEFAULT_LOG_FILE_NAME

    # Ensure log directory exists
    log_file_path = Path(log_file_path_str)
    if log_file_path.is_absolute():
        log_dir = log_file_path.parent
    else:
        # If relative, assume it's relative to a standard log directory or project root
        # For simplicity, let's make it relative to DEFAULT_LOG_DIR if not absolute
        log_dir = DEFAULT_LOG_DIR
        log_file_path = log_dir / log_file_path_str

    log_dir.mkdir(parents=True, exist_ok=True)


    numeric_log_level = getattr(logging, log_level_str, logging.INFO)

    # Get the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_log_level)

    # Remove any existing handlers to avoid duplicate logs if setup_logging is called multiple times
    # (e.g., in tests or re-initializations)
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()


    # Console Handler (Pretty Print)
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(numeric_log_level) # Console also respects the configured level
    root_logger.addHandler(console_handler)

    # File Handler (JSON)
    # Use RotatingFileHandler for managing log file sizes
    # Max 5MB per file, keep 5 backup files.
    file_handler = RotatingFileHandler(
        log_file_path, maxBytes=5*1024*1024, backupCount=5
    )
    # Custom formatter for JSON logs, includes standard fields
    # See python-json-logger documentation for more fields like exc_info for exceptions
    json_formatter = jsonlogger.JsonFormatter(
        '%(asctime)s %(levelname)s %(name)s %(module)s %(funcName)s %(lineno)d %(message)s'
    )
    file_handler.setFormatter(json_formatter)
    file_handler.setLevel(numeric_log_level) # File also respects the configured level
    root_logger.addHandler(file_handler)

    logging.info(f"Logging setup complete. Level: {log_level_str}. Log file: {log_file_path.resolve()}")
    logging.debug("This is a debug message and will only appear if log level is DEBUG.")


if __name__ == '__main__': # pragma: no cover
    # Example of using the setup_logging function

    # 1. Basic setup (no config)
    print("--- Testing basic logging setup (INFO level, default file) ---")
    setup_logging()
    logging.info("Info message after basic setup.")
    logging.warning("Warning message after basic setup.")
    logging.debug("Debug message after basic setup (should not appear).")
    # Check the default log file (e.g., ./logs/delta_rollover_bot.log or delta_rollover_bot.log)

    print("\n--- Testing logging setup with dummy config (DEBUG level) ---")
    # 2. Setup with a dummy config
    class DummyConfigLog:
        def get(self, key, default=None):
            if key == "logging.log_level":
                return "DEBUG"
            if key == "logging.log_file":
                return "test_bot_debug.log" # Will be placed in ./logs/
            return default

    dummy_config = DummyConfigLog()
    setup_logging(config=dummy_config)
    logging.info("Info message after DEBUG setup.")
    logging.warning("Warning message after DEBUG setup.")
    logging.debug("Debug message after DEBUG setup (SHOULD appear).")
    # Check ./logs/test_bot_debug.log

    # Example of logging an exception (will include exc_info in JSON)
    try:
        x = 1 / 0
    except ZeroDivisionError:
        logging.error("A ZeroDivisionError occurred!", exc_info=True)

    logging.info("To see JSON logs, check the .log files created in the current directory or ./logs/ directory.")
