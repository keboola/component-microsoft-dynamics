import sys
from dynamics.component import DynamicsComponent

# Environment setup
sys.tracebacklimit = 0

if __name__ == '__main__':

    c = DynamicsComponent()
    c.run()
