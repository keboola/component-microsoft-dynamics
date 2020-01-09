import logging
import os
import sys
import logging_gelf.handlers
import logging_gelf.formatters
# from dynamics.client import DynamicsClient
# from dynamics.result import DynamicsWriter
from dynamics.component import DynamicsComponent

# Environment setup
sys.tracebacklimit = 3

# Logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)-8s : [line:%(lineno)3s] %(message)s',
    datefmt="%Y-%m-%d %H:%M:%S")

if 'KBC_LOGGER_ADDR' in os.environ and 'KBC_LOGGER_PORT' in os.environ:

    logger = logging.getLogger()
    logging_gelf_handler = logging_gelf.handlers.GELFTCPSocketHandler(
        host=os.getenv('KBC_LOGGER_ADDR'),
        port=int(os.getenv('KBC_LOGGER_PORT'))
    )
    logging_gelf_handler.setFormatter(
        logging_gelf.formatters.GELFFormatter(null_character=True))
    logger.addHandler(logging_gelf_handler)

    # removes the initial stdout logging
    logger.removeHandler(logger.handlers[0])

APP_VERSION = '0.0.1'

if __name__ == '__main__':

    logging.info("Running component version %s..." % APP_VERSION)

    c = DynamicsComponent()
    c.run()

    logging.info("Extraction finished.")
