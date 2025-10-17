import logging
import sys

TRACE = 5
# DEBUG = 10
VERBOSE = 15
# INFO = 20
NOTICE = 25
# WARNING = 30
# ERROR = 40
# CRITICAL = 50

logging.addLevelName(TRACE, 'TRACE')
logging.addLevelName(VERBOSE, 'VERBOSE')
logging.addLevelName(NOTICE, 'NOTICE')

def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE):
        self._log(TRACE, message, args, **kwargs)

def verbose(self, message, *args, **kwargs):
    if self.isEnabledFor(VERBOSE):
        self._log(VERBOSE, message, args, **kwargs)

def notice(self, message, *args, **kwargs):
    if self.isEnabledFor(NOTICE):
        self._log(NOTICE, message, args, **kwargs)

logging.Logger.trace = trace
logging.Logger.verbose = verbose
logging.Logger.notice = notice


logger = logging.getLogger('airflow_pipeline')
logger.setLevel(logging.DEBUG)  # global threshold

# Avoid duplicate handlers if imported multiple times
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '[%(asctime)s] %(levelname)s - %(name)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False


def get_logger(name=None):
    """Return the main logger or a child logger with a qualified name."""
    return logger.getChild(name) if name else logger
