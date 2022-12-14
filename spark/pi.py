#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
from random import random
from operator import add
#import pymongo

#mongo_client = pymongo.MongoClient("mongodb://mongodb/", username='root', password='AUKr2OhZ4z7FS5nZC0lZ')
#db = mongo_client["tradebots-db"]
#test_collection = db['test_collection']
#test_collection.insert_one({'pi': 'uwu'})

from pyspark.sql import SparkSession
from pyspark import SparkContext,SparkConf


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """

    def init_spark():
        mongo_conn = f"mongodb://root:AUKr2OhZ4z7FS5nZC0lZ@mongodb:27017/ayaya.coll"

        conf = SparkConf()

        # Download mongo-spark-connector and its dependencies.
        # This will download all the necessary jars and put them in your $HOME/.ivy2/jars, no need to manually download them :
        conf.set("spark.jars.packages",
                "org.mongodb.spark:mongo-spark-connector:10.0.1")

        # Set up read connection :
        conf.set("spark.mongodb.read.connection.uri", mongo_conn)
        conf.set("spark.mongodb.read.database", "ayaya")
        conf.set("spark.mongodb.read.collection", "col")

        # Set up write connection
        conf.set("spark.mongodb.write.connection.uri", mongo_conn)
        conf.set("spark.mongodb.write.database", "ayaya")
        conf.set("spark.mongodb.write.collection", "col")
        # If you need to update instead of inserting :
        conf.set("spark.mongodb.write.operationType", "update")

        SparkContext(conf=conf)

        return SparkSession \
            .builder \
            .appName('PythonPi') \
            .getOrCreate()

    spark = init_spark()
    
    people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
    people.write.format("mongodb").mode("append").save()


    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    pi = 4.0 * count / n
    print("Pi is roughly %f" % pi)
    #test_collection.insert_one({'pi': pi})

    spark.stop()
