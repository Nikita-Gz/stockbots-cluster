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

from pyspark.sql import SparkSession

import pymongo
import json
import os


if __name__ == "__main__":
    """
        Usage: pi [partitions]
    """

    mongo_client = pymongo.MongoClient("mongodb://mongodb/", username='root', password=os.getenv('MONGODB_ROOT_PASSWORD'))
    db = mongo_client["kekeke"]
    realtime_prices_collection = db['realtime-prices-finnhub']

    spark = SparkSession\
        .builder\
        .appName("PythonPi")\
        .getOrCreate()
        #.config("spark.mongodb.output.uri", "mongodb://root:AUKr2OhZ4z7FS5nZC0lZ@mongodb/test.coll")\
        #.config("spark.mongodb.input.uri", "mongodb://root:AUKr2OhZ4z7FS5nZC0lZ@mongodb/test.coll")\
        #.getOrCreate()
        #.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.11:2.3.2")\
        #.getOrCreate()

    #def save_json_row_to_mongo(json_string):
    #    json_object = json.loads(json_string)
    #    realtime_prices_collection.insert_one(json_object)

    people = spark.createDataFrame([("Bilbo Baggins",  50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77),
    ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
    as_pandas = people.toPandas()
    records = as_pandas.to_dict('records')

    realtime_prices_collection.insert_many(records)
    #people.write.json("/opt/spark/work-dir/db.json")
    #files = os.listdir("/opt/spark/work-dir/db.json/")
    #files = [f for f in files if os.path.isfile("/opt/spark/work-dir/"+'/'+f)] #Filtering only the files.
    #print(files)
    #with open("/opt/spark/work-dir/db.json", 'r') as file:
    #    print(json.load(file))
    #people.toJSON().foreach(lambda json_string: realtime_prices_collection.insert_one({'a': 123}))
    #people.write.format("mongodb").mode("append").save()


    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    n = 100000 * partitions

    def f(_: int) -> float:
        x = random() * 2 - 1
        y = random() * 2 - 1
        return 1 if x ** 2 + y ** 2 <= 1 else 0

    count = spark.sparkContext.parallelize(range(1, n + 1), partitions).map(f).reduce(add)
    print("="*20 + "Pi is roughly %f" % (4.0 * count / n))

    spark.stop()

