import os
import pymongo

def connect_to_mongo_db(db_name='tradebots-db'):
  mongo_client = pymongo.MongoClient("mongodb://mongodb/", username='root', password=os.getenv('MONGODB_ROOT_PASSWORD'))
  db = mongo_client[db_name]
  return db
