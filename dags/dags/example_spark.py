import datetime
from operator import add
from pyspark.sql import SparkSession, functions
from pyspark import SparkConf, SparkContext
import sys
from random import random

# As configurações são recebidas pelo comando spark-submit executado no SparkSubmitOperator
spark = SparkSession(SparkContext(conf=SparkConf()).getOrCreate())

partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
n = 100000 * partitions

def f(_: int) -> float:
    x = random() * 2 - 1
    y = random() * 2 - 1
    return 1 if x ** 2 + y ** 2 <= 1 else 0

count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
print("="*20 + "Pi is roughly %f" % (4.0 * count / n))

args = sys.argv[1:]
date_to_process = args[0]
print(date_to_process)

spark.stop()
