import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import MetaData, create_engine, extract, select
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import or_
import datetime

from src.data.processing_func import (get_direction)
from src.data.load_func import gen_jps_dataset

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

date_begin = datetime.date(day=1, month=11, year=2017)
date_end = datetime.date(day=15, month=1, year=2018)
morn_start = 9
morn_end = 11
aft_start = 19
aft_end = 21

df = gen_jps_dataset(meta, date_begin, date_end, morn_start, morn_end,
                    aft_start, aft_end, weekends=True, summary=True)

