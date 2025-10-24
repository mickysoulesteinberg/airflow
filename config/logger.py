import logging
import sys

# Custom log levels (with standard ones for reference)
MICRO = 2
TRACE = 5
# DEBUG = 10
VERBOSE = 15
# INFO = 20
NOTICE = 25
# WARNING = 30
# ERROR = 40
# CRITICAL = 50

logging.MICRO = MICRO
logging.TRACE = TRACE
logging.VERBOSE = VERBOSE
logging.NOTICE = NOTICE

logging.addLevelName(MICRO, 'MICRO')
logging.addLevelName(TRACE, 'TRACE')
logging.addLevelName(VERBOSE, 'VERBOSE')
logging.addLevelName(NOTICE, 'NOTICE')

# Extend logger with custom levels
def micro(self, message, *args, **kwargs):
    if self.isEnabledFor(MICRO):
        self._log(MICRO, message, args, **kwargs)

def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)

def verbose(self, message, *args, **kwargs):
    if self.isEnabledFor(VERBOSE):
        self._log(VERBOSE, message, args, **kwargs)

def notice(self, message, *args, **kwargs):
    if self.isEnabledFor(NOTICE):
        self._log(NOTICE, message, args, **kwargs)

logging.Logger.micro = micro
logging.Logger.trace = trace
logging.Logger.verbose = verbose
logging.Logger.notice = notice


# Color formatting
class ColorFormatter(logging.Formatter):
    COLORS = {
        MICRO:    '\033[38;5;244m',  # light gray
        TRACE:    '\033[38;5;240m',  # gray
        logging.DEBUG: '\033[38;5;111m', # light blue
        VERBOSE:  '\033[38;5;63m',  # blue
        logging.INFO: '\033[38;5;2m',  # green
        NOTICE:   '\033[38;5;220m',  # yellow
        logging.WARNING: '\033[38;5;208m',  # orange
        logging.ERROR: '\033[38;5;1m',  # maroon
        logging.CRITICAL: '\033[1;196m',  # light red background
    }
    RESET = '\033[0m'

    def format(self, record):
        color = self.COLORS.get(record.levelno, self.RESET)
        record.levelname = f'{color}{record.levelname}{self.RESET}'
        record.msg = f'{color}{record.msg}{self.RESET}'
        return super().format(record)


# Logger setup
logger = logging.getLogger('names')
logger.setLevel(logging.DEBUG)  # global threshold

# Avoid duplicate handlers if imported multiple times
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = ColorFormatter(
        '[%(asctime)s] %(levelname)-9s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


# Access logger
def get_logger(name=None):
    """Return the main logger or a child logger with a qualified name."""
    return logger.getChild(name) if name else logger
