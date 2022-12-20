# Runs several mlflow logging experiments periodically (every 30 seconds).
# Also measures time taken to run each experiment.

import time
import datetime
from experiment1 import run_experiment1

if __name__ == "__main__":
  while True:
    print("Running experiment1...")
    start = datetime.datetime.now()
    run_experiment1()
    end = datetime.datetime.now()
    print("Time taken to run experiment1: " + str(end - start))
    time.sleep(30)
