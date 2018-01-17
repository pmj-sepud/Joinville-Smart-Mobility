import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import json
from io import StringIO
import geopandas as gpd

from sqlalchemy import create_engine, exc, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import JSON as typeJSON

from src.data.processing_func import tabulate_records, prep_rawdata_tosql, tabulate_jams, prep_jams_tosql

project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

#Connection and initial setup
DATABASE = {
    'drivername': os.environ.get("db_drivername"),
    'host': os.environ.get("db_host"), 
    'port': os.environ.get("db_port"),
    'username': os.environ.get("db_username"),
    'password': os.environ.get("db_password"),
    'database': os.environ.get("db_database"),
}

db_url = URL(**DATABASE)
engine = create_engine(db_url)
meta = MetaData()
meta.bind = engine
meta.reflect()

#Store Mongo Record info
mongo_record = meta.tables["MongoRecord"]
mongo_record.delete().execute()
file = open(project_dir + "/data/raw/waze_rawdata.txt", "r")
json_string = json.load(file)
json_io = StringIO(json_string)
records = json.load(json_io)
raw_data = tabulate_records(records)
rawdata_tosql = prep_rawdata_tosql(raw_data)
rawdata_tosql.to_sql("MongoRecord", con=meta.bind, if_exists="append", index=False)


#Build dataframe of jams and store in PostgreSQL
df_jams = tabulate_jams(raw_data)
jam = meta.tables["Jam"]
jam.delete().execute()
jams_tosql = prep_jams_tosql(df_jams)
jams_tosql.to_sql("Jam", con=meta.bind, if_exists="append", index=False,
                 dtype={"JamDscCoordinatesLonLat": typeJSON, 
                        "JamDscSegments": typeJSON
                       }
                 )

#Build dataframe of alerts and store in PostgreSQL

#Build dataframe of irregularities and store in PostgreSQL

