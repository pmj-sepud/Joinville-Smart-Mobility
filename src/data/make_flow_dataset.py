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
from src.data.load_func import extract_jps, transf_flow_features, transf_flow_labels

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

date_begin = datetime.date(day=1, month=11, year=2017)
date_end = datetime.date(day=15, month=1, year=2018)

#df_jps = gen_df_jps(meta, date_begin, date_end, periods=[(7,9), (17,19)], weekends=True, summary=True)
df_jps = extract_jps(meta, datetime.date(day=1, month=1, year=2018), date_end, weekends=True, summary=True)
df_flow_features = transf_flow_features(df_jps)
path_fluxos = project_dir + "/data/external/fotosensores_Fluxo_veiculos.xlsx"
df_flow_labels = transf_flow_labels(meta, path_fluxos)

df_flow_features_NS = df_flow_features.reset_index(level="LonDirection")
df_flow_features_NS.index.rename("Direction", level="LatDirection", inplace=True)

df_flow_features_LW = df_flow_features.reset_index(level="LatDirection")
df_flow_features_LW.index.rename("Direction", level="LonDirection", inplace=True)

try:
	df_flow_features_NS = df_flow_features_NS.merge(df_flow_labels, how="inner", left_index=True, right_index=True)
	df_flow_features_NS.reset_index(inplace=True)
except TypeError as e:
	print(str(e))

try:
	df_flow_features_LW = df_flow_features_LW.merge(df_flow_labels, how="inner", left_index=True, right_index=True)
	df_flow_features_LW.reset_index(inplace=True)
except TypeError  as e:
	print(str(e))

if ('flow_dataset_NS' in globals()) & ('flow_dataset_LW' in globals()):
	flow_dataset = df_flow_features_NS.append(df_flow_features_LW, ignore_index=True)
elif 'flow_dataset_NS' in globals():
	df_flow_features_NS.to_csv(project_dir + "/data/processed/flow_dataset.csv")
elif 'flow_dataset_LW' in globals():
	df_flow_features_LW.to_csv(project_dir + "/data/processed/flow_dataset.csv")
