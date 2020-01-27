import logging
import sys
from dynamics.component import DynamicsComponent

# Environment setup
sys.tracebacklimit = 0

APP_VERSION = '0.0.1'

if __name__ == '__main__':

    c = DynamicsComponent()
    c.run()

    logging.info("Extraction finished.")
