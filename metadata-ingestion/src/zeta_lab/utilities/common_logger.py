import logging

def setup_logging(log_file, logger_name=None):
    """
    Set up logging configuration for both console and file output.
    
    :param log_file: Path to the log file
    :param logger_name: Name of the logger (optional)
    :return: Configured logger instance
    """
    logger = logging.getLogger(logger_name if logger_name else __name__)
    logger.setLevel(logging.INFO)
    
    # Create formatters
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file, mode='a', encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    return logger

def cleanup_logger(logger):
    """
    Clean up logger handlers to avoid duplicate logging.
    
    :param logger: Logger instance to clean up
    """
    for handler in logger.handlers[:]:
        handler.close()
        logger.removeHandler(handler)
