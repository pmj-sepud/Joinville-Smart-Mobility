import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import geopandas as gpd
import pandas as pd
import math
from timeit import default_timer as timer

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TIMESTAMP as typeTIMESTAMP

from src.data.processing_func import (extract_geo_jams, extract_geo_sections)

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

#Flush JamPersection and Build geo_sections
jps = meta.tables["JamPerSection"]
jps.delete().execute()
geo_sections = extract_geo_sections(meta)

##Divide the in batches
batch_size = 20000 #arbitrary number to make the code tractable
total_rows, = meta.tables["Jam"].count().execute().first()
number_batches = math.ceil(total_rows / batch_size)

for i in range(0, number_batches):
    start = timer()
    #Build and store JamPerSection
    geo_jams = extract_geo_jams(meta, skip=i*batch_size, limit=batch_size)
    jams_per_section = gpd.sjoin(geo_jams, geo_sections, how="inner", op="contains")
    jams_per_section = jams_per_section[["JamDateStart", "JamUuid", "SctnId"]]  
    jams_per_section["JamDateStart"] = jams_per_section["JamDateStart"].astype(pd.Timestamp)
    jams_per_section.to_sql("JamPerSection", con=meta.bind, if_exists="append", index=False)
    end = timer()
    duration = str(round(end - start))
    print("Batch " + str(i+1) + " of " + str(number_batches) + " took " + duration + " s to be successfully stored.")
