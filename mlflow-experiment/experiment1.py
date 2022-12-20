import mlflow
import os
from random import random, randint
from mlflow import log_metric, log_param, log_artifacts

def run_experiment1():
    mlflow.set_tracking_uri("http://mlflow-tracking:5000")
    mlflow.set_experiment("my-experiment1")

    with mlflow.start_run():
        # Log a parameter (key-value pair)
        log_param("param1", randint(0, 100))

        log_param('tracking uri', mlflow.get_tracking_uri())
        log_param('artifact uri', mlflow.get_artifact_uri())

        # Log a metric; metrics can be updated throughout the run
        log_metric("foo", random())
        log_metric("foo", random() + 1)
        log_metric("foo", random() + 2)

        # Log an artifact (output file)
        if not os.path.exists("./outputs"):
            os.makedirs("outputs")
        with open("./outputs/test.txt", "w") as f:
            f.write("hello world!")
        with open("./outputs/test2.txt", "w") as f:
            f.write("hello world?")
        log_artifacts('outputs')

