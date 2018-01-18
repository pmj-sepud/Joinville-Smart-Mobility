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
from pytz import timezone

from src.data.processing_func import (get_direction)
from src.data.load_func import gen_df_jps

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

timezone = os.environ.get("timezone")
db_url = URL(**DATABASE)
engine = create_engine(db_url, connect_args={"options": "-c timezone="+timezone})
meta = MetaData()
meta.bind = engine
meta.reflect()

date_begin = datetime.date(day=1, month=11, year=2017)
date_end = datetime.date(day=15, month=1, year=2018)

df_jps = gen_df_jps(meta, date_begin, date_end, periods=[(7,9), (17,19)], weekends=True, summary=True)



