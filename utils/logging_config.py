# utils/logging_config.py
import logging

class ContextualLogFormatter(logging.Formatter):
    def format(self, record):
        # Set a default for org_group if not present in the log record
        if not hasattr(record, 'org_group'):
            record.org_group = '------'  # Default 6-character value
        else:
            # Ensure org_group is at least 6 characters (pad if shorter, keep if longer)
            org_group_str = str(record.org_group)
            if len(org_group_str) < 6:
                record.org_group = org_group_str.ljust(6)  # Pad with spaces to minimum 6 chars
            # If 6 or more chars, leave as is
        
        return super().format(record)

def setup_global_logging(log_level_str="INFO", log_file="logs/generate_codejson_main.log"):
    """
    Configures global logging with a contextual formatter.
    """
    import os # For path operations if creating log directory

    # Ensure logs directory exists (optional, if your main script doesn't do this)
    # log_dir = os.path.dirname(log_file)
    # if log_dir and not os.path.exists(log_dir):
    #     os.makedirs(log_dir, exist_ok=True)

    log_level = getattr(logging, log_level_str.upper(), logging.INFO)

    # New log format including the organization/group
    log_format = '%(asctime)s - [%(org_group)s] - %(name)s - %(levelname)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    formatter = ContextualLogFormatter(log_format, datefmt=date_format)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to prevent duplicate messages if this setup is called multiple times
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # File Handler (if you are using one)
    # Ensure you have permissions and the directory exists if using a file handler
    try:
        file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    except Exception as e:
        logging.error(f"Failed to set up file handler for {log_file}: {e}", exc_info=True)


    # Use the root logger for this initial message
    logging.getLogger().info("Global logging configured with contextual formatter.")

