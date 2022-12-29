# 1) gets data
# 2) runs experiments
# 3) sleeps for 1 hour
# 4) goes back to 1)

import time

from run_experiments import run_experiment
from data_preparation import prepare_data_for_prediction


def main():
  while True:
    data = prepare_data_for_prediction()
    run_experiment(data)
    time.sleep(60 * 60)


if __name__ == '__main__':
  main()
