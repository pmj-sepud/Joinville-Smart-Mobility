import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import MetaData, create_engine, extract, select
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import or_
import datetime
from pytz import timezone

from src.data.processing_func import (get_direction)
from src.data.load_func import (extract_jps,
								transf_flow_features,
								transf_flow_labels,
								transf_traffic_per_timeslot,
								transf_probability_matrix)

import dotenv
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

#Generate probability and criticity indicators for sections of interest
sections_interest = pd.read_csv(project_dir + "/data/external/vias_estudo.csv", index_col=0, decimal=',')

date_begin = datetime.date(day=14, month=10, year=2017)
date_end = datetime.date(day=15, month=10, year=2017)

periods = [(7,9), (17,19)]

df_jps = extract_jps(meta, date_begin, date_end, periods=periods, weekends=True, summary=True)

holidays = pd.read_excel(project_dir + "/data/external/feriados_nacionais.xls", skip_footer=9)
holidays["Data"] = holidays["Data"].dt.date
holiday_list = holidays["Data"].tolist()

geo_jps_per_timeslot = transf_traffic_per_timeslot(df_jps, meta, holiday_list)
prob_matrix = transf_probability_matrix(geo_jps_per_timeslot, sections_interest)
#traffic_indicators = gen_traffic_indicators(prob_matrix)

#geo_jps_per_timeslot.sort_index(inplace=True)
#geo_jps_per_timeslot.reset_index(inplace=True)
#geo_jps_per_timeslot.to_csv(project_dir + "/data/interim/jps_per_timeslot.csv")

import pdb
pdb.set_trace()