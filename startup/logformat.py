import logging

# NOTE
# partial steal from https://betterstack.com/community/questions/how-to-color-python-logging-output/
class LogFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning / errors"""

    # NOTE
    # color reference: https://en.wikipedia.org/wiki/ANSI_escape_code#Colors
    faint_grey = "\x1b[38;5;8m"
    grey = "\x1b[38;5;7m"
    yellow = "\x1b[38;5;172m"
    pink_red = "\x1b[38;5;197m"
    red_background = "\x1b[1;37;41m"

    reset = "\x1b[0m"

    format = "%(asctime)s | %(module)s:%(lineno)d:%(levelname)-8s %(message)s"

    FORMATS = {
        logging.DEBUG: faint_grey + format + reset,
        logging.INFO: grey + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: pink_red + format + reset,
        logging.CRITICAL: red_background + format + reset,
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)