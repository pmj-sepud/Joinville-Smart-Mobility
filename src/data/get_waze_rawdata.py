import os
import sys
import dotenv
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

import json
from bson.json_util import dumps
import math
from timeit import default_timer as timer
import argparse

from pymongo import MongoClient, DESCENDING, ASCENDING
from processing_func import collect_records

parser = argparse.ArgumentParser(description="Collect Waze's raw data")
parser.add_argument('batchsize', type=int, help="Size of download batches")
parser.add_argument('update', type=bool, nargs='?', const=False,
                    help="""If true, delete all documents and download them again.
                    If false, continue from the last document""")

args = parser.parse_args()
batch_size = args.batchsize
update = args.update

all_files = os.listdir(project_dir+"/data/raw/")
all_files = [file for file in all_files if "_.txt" in file]

num_docs = 0
if update:
    for file in all_files:
        os.remove(project_dir + "/data/raw/" + file)
else:
    for file in all_files:
        file_info = file.split("_")
        num_docs += int(file_info[-2])

#MongoDB Connection
uri = os.environ.get("mongo_uri")
client = MongoClient(uri)
db = client.ccp
collection = db.ccp_collection

#Divide the code by batches
total_rows = collection.count()
number_batches = math.ceil(total_rows / batch_size)
width = len(str(number_batches))

for i in range(0, number_batches):
    start = timer()
    #Fetch documents
    records = list(collection.find({}, skip=(num_docs + batch_size*i), limit=batch_size, sort=[("_id", ASCENDING)]))

    with open(project_dir+"/data/raw/wazerawdata_" + str(i+1).zfill(width) +
             "_of_" + str(number_batches) + "_batch_" + str(batch_size) + 
             "_.txt", 'w') as outfile:
        json_record = dumps(records)
        json.dump(json_record, outfile)

    end = timer()
    duration = str(round(end - start))
    print("Batch " + str(i+1) + " of " + str(number_batches) + " took " + duration + " s to be successfully stored.")






