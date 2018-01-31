import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import json
from io import StringIO
import geopandas as gpd
from timeit import default_timer as timer

from sqlalchemy import create_engine, exc, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import JSON as typeJSON

from src.data.processing_func import (tabulate_records, prep_rawdata_tosql, tabulate_jams,
                                     prep_jams_tosql, connect_database)

project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

#Connection and initial setup
meta = connect_database()

#Flush database tables
flush = None
while (flush != "Y") and (flush != "N") and (flush != "y") and (flush != "n"):
    flush = input("Are you sure you want to flush MongoRecord and Jam tables? (Y/N): ")
    if (flush == "Y") or (flush == "y"):
        print("Flushing tables...")
        mongo_record = meta.tables["MongoRecord"]
        jam = meta.tables["Jam"]
        mongo_record.delete().execute()
        jam.delete().execute()
    elif (flush == "N") or (flush == "n"):
        print("Aborting operation...")
        sys.exit()

#Store Mongo Record info
all_files = os.listdir(project_dir+"/data/raw/")
all_files = [file for file in all_files if "_.txt" in file]
all_files.sort()

for filename in all_files:
    start = timer()
    doc = open(project_dir + "/data/raw/" + filename, "r")
    json_string = json.load(doc)
    json_io = StringIO(json_string)
    records = json.load(json_io)

    #Store MongoRecords 
    raw_data = tabulate_records(records)
    rawdata_tosql = prep_rawdata_tosql(raw_data)
    rawdata_tosql.to_sql("MongoRecord", con=meta.bind, if_exists="append", index=False)

    #Build dataframe of jams and store in PostgreSQL
    df_jams = tabulate_jams(raw_data)
    jams_tosql = prep_jams_tosql(df_jams)
    jams_tosql.to_sql("Jam", con=meta.bind, if_exists="append", index=False,
                     dtype={"JamDscCoordinatesLonLat": typeJSON, 
                            "JamDscSegments": typeJSON
                           }
                     )
    end = timer()
    duration = str(round(end - start))
    file_info = filename.split("_")
    batch_number = file_info[1]
    batch_size = file_info[-2]

    print("Stored jams from batch " + batch_number + ", with a size of " + batch_size + " documents, in " + duration + " seconds.")

    #Build dataframe of alerts and store in PostgreSQL

    #Build dataframe of irregularities and store in PostgreSQL
