import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import json
from io import StringIO

from sqlalchemy import create_engine, Column, Integer, DateTime, UniqueConstraint, exc
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect, MetaData
from sqlalchemy.orm import sessionmaker

from src.data.functions import tabulate_records, prep_rawdata_tosql

project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

#Connection and initial setup
DATABASE = {
    'drivername': os.environ.get("test_db_drivername"),
    'host': os.environ.get("test_db_host"), 
    'port': os.environ.get("test_db_port"),
    'username': os.environ.get("test_db_username"),
    'password': os.environ.get("test_db_password"),
    'database': os.environ.get("test_db_database"),
}

db_url = URL(**DATABASE)
engine = create_engine(db_url)
Base = declarative_base()
meta = MetaData()
meta.bind = engine
meta.reflect()

#Store Mongo Record info
file = open(project_dir + "/data/raw/waze_rawdata.txt", "r")
json_string = json.load(file)
json_io = StringIO(json_string)
records = json.load(json_io)
raw_data = tabulate_records(records)
rawdata_tosql = prep_rawdata_tosql(raw_data)

try:
  rawdata_tosql.to_sql("MongoRecord", con=meta.bind, if_exists="replace", index=False)
except (exc.IntegrityError):
    print("MongoRecord already stored")