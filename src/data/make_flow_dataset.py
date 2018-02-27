import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import math
import pandas as pd
import geopandas as gpd
from sqlalchemy import MetaData, create_engine, extract, select
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import or_
import datetime
from pytz import timezone
import glob

from timeit import default_timer as timer

from src.data.processing_func import (get_direction, connect_database, extract_geo_sections)
from src.data.load_func import extract_jps, transf_flow_features, transf_flow_labels

import dotenv
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

rewrite_files = None
while (rewrite_files != "Y") and (rewrite_files != "N") and (rewrite_files != "y") and (rewrite_files != "n"):
    rewrite_files = input("Do you wish to overwrite flow_dataset files? (Y/N): ")
if (rewrite_files == "N") or (rewrite_files == "n"):
    pass
elif (rewrite_files == "Y") or (rewrite_files == "y"):
    filenames = glob.glob(project_dir + "/data/test/flow_dataset_*.csv")
    for f in filenames:
        os.remove(f)

#Connection and initial setup
DATABASE = {
'drivername': os.environ.get("db_drivername"),
'host': os.environ.get("db_host"), 
'port': os.environ.get("db_port"),
'username': os.environ.get("db_username"),
'password': os.environ.get("db_password"),
'database': os.environ.get("db_database"),
}

meta = connect_database(DATABASE)

geo_sections = extract_geo_sections(meta)
geo_sections.set_index("SctnId", inplace=True)

path_fluxos = project_dir + "/data/external/fotosensores_Fluxo_veiculos.csv"
df_flow_labels = transf_flow_labels(geo_sections, path_fluxos)

date_begin = datetime.date(day=1, month=9, year=2017)
date_end = datetime.date(day=31, month=1, year=2018)

total_rows = extract_jps(meta, date_begin, date_end, weekends=True, summary=True, return_count=True)
batch_size = 50000
number_batches = math.ceil(total_rows / batch_size)
width = len(str(number_batches))

for i in range(0, number_batches):
    if glob.glob(project_dir + "/data/test/flow_dataset_" + str(i+1).zfill(width) + ".csv"):
        continue

    df_jps = extract_jps(meta, date_begin, date_end, weekends=True, summary=True, skip=i*batch_size, limit=batch_size)

    start = timer()
    df_flow_features = transf_flow_features(df_jps, geo_sections)

    try:
        flow_dataset = df_flow_features.merge(df_flow_labels, how="inner", left_index=True, right_index=True)
    except TypeError as e:
        print(e)
        continue
    
    end = timer()
    duration = str(round(end - start))
    num_matches = len(flow_dataset)
    flow_dataset.to_csv(project_dir + "/data/test/flow_dataset_" + str(i+1).zfill(width) + ".csv")
    print("Batch " + str(i+1) + " of " + str(number_batches) + " took " + \
          duration + "s to process " + \
          str(num_matches) + \
          " matches, and document was successfully stored")







