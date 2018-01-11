import os
import sys
import dotenv
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

import json
from bson.json_util import dumps

from pymongo import MongoClient, DESCENDING, ASCENDING
from functions import collect_records

#MongoDB Connection
uri = os.environ.get("mongo_uri")
client = MongoClient(uri)
db = client.ccp
collection = db.ccp_collection

limit=5
records = collect_records(collection, limit)

with open(project_dir+"/data/raw/waze_rawdata.txt", 'w') as outfile:
	json_record = dumps(records)
	json.dump(json_record, outfile)






