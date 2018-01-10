import sys
import os

import pandas as pd
from pymongo import MongoClient
import requests
import functions

from sqlalchemy import *
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import JSON as typeJSON
from sqlalchemy import exc
import exceptions

import dotenv

project_dir = os.path.join(os.path.dirname(__file__), os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

#Collect Data
r = os.environ.get("waze_url")

record = r.json()

#Insert in MongoDB
uri = os.environ.get("mongo_uri") #change password and include in ENV VARIABLE
client = MongoClient(uri)
db = client.ccp
collection = db.ccp_collection

try:    
    result = collection.insert(record)
except:
    print("Document already stored")

#Connection and initial setup
DATABASE = {
    'drivername': os.environ.get("db_drivername"),
    'host': os.environ.get("db_host"), 
    'port': os.environ.get("db_port"),
    'username': os.environ.get("db_username"),
    'password': os.environ.get("db_password"), #change password and include in ENV VARIABLE
    'database': os.environ.get("db_database"),
}
db_url = URL(**DATABASE)
engine = create_engine(db_url)
meta = MetaData()
meta.bind = engine
meta.reflect()

#Store Mongo Record info
records = [record]
raw_data = functions.tabulate_records(records)
rawdata_tosql = functions.prep_rawdata_tosql(raw_data)
try:
  rawdata_tosql.to_sql("MongoRecord", con=meta.bind, if_exists="append", index=False)
except (exc.IntegrityError):
    print("MongoRecord already stored")

#Build dataframe
try:
  df_jams = functions.build_df_jams(raw_data)
except exceptions.NoJamError:
    print("No Jam in the given period")
    sys.exit()

jams_tosql = functions.prep_jams_tosql(df_jams)

#Append jam in "Jam" table
try:    
    jams_tosql.to_sql("Jam", con=meta.bind, if_exists="append", index=False, dtype={"JamDscCoordinatesLonLat": typeJSON,                                                                                   "JamDscSegments": typeJSON})
except (exc.IntegrityError):
    print("Jam already stored")

#Build df_logr e df_trechos
lws = meta.tables['LkpWazeSepud']
logr_query = lws.select()
df_logr = pd.read_sql(logr_query, con=meta.bind, index_col="LwsId")

df_trechos = functions.build_df_trechos(meta)

#Append jams_per_trecho
df_jams = pd.merge(df_jams, df_logr, left_on="jams_street", right_on="LwsDscWazeStreet", how="left")
df_jams['impacted_trechos'] = df_jams.apply(lambda x: functions.get_impacted_trechos(x, df_trechos), axis=1)
jams_per_trecho = functions.explode_impacted_trechos(df_jams)
jpt_tosql = functions.prep_jpt_tosql(jams_per_trecho)
   
try:
  jpt_tosql.to_sql("JamPerTrecho", con=meta.bind, if_exists="append", index=False)
except (exc.IntegrityError):
    print("JamPerTrecho already stored")
