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

from src.data.processing_func import (get_direction, connect_database)
from src.data.load_func import extract_jps, transf_flow_features, transf_flow_labels

import dotenv
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

#Connection and initial setup
meta = connect_database()

date_begin = datetime.date(day=15, month=10, year=2017)
date_end = datetime.date(day=20, month=10, year=2017)

df_jps = extract_jps(meta, date_begin, date_end, weekends=True, summary=True)
df_flow_features = transf_flow_features(df_jps)
path_fluxos = project_dir + "/data/external/fotosensores_Fluxo_veiculos.csv"
df_flow_labels = transf_flow_labels(meta, path_fluxos)

df_flow_features_NS = df_flow_features.reset_index(level="LonDirection")
df_flow_features_NS.index.rename("Direction", level="LatDirection", inplace=True)

df_flow_features_LW = df_flow_features.reset_index(level="LatDirection")
df_flow_features_LW.index.rename("Direction", level="LonDirection", inplace=True)

try:
	df_flow_dataset_NS = df_flow_features_NS.merge(df_flow_labels, how="inner", left_index=True, right_index=True)
	df_flow_dataset_NS.reset_index(inplace=True)
except TypeError as e:
	print(str(e))

try:
	df_flow_dataset_LW = df_flow_features_LW.merge(df_flow_labels, how="inner", left_index=True, right_index=True)
	df_flow_dataset_LW.reset_index(inplace=True)
except TypeError  as e:
	print(str(e))

if ('df_flow_dataset_NS' in globals()) & ('df_flow_dataset_LW' in globals()):
	flow_dataset = df_flow_dataset_NS.append(df_flow_dataset_LW, ignore_index=True)
	flow_dataset.to_csv(project_dir + "/data/processed/flow_dataset.csv")
elif 'df_flow_dataset_NS' in globals():
	df_flow_dataset_NS.to_csv(project_dir + "/data/processed/flow_dataset.csv")
elif 'df_flow_dataset_LW' in globals():
	df_flow_dataset_LW.to_csv(project_dir + "/data/processed/flow_dataset.csv")