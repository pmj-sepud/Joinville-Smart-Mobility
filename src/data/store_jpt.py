import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import json
from io import StringIO
import geopandas as gpd

from sqlalchemy import create_engine, Column, Integer, DateTime, UniqueConstraint, exc
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import JSON as typeJSON

from src.data.functions import (tabulate_records, prep_rawdata_tosql, build_geo_jams,
                                prep_jams_tosql, build_geo_trechos, prep_jpt_tosql)

project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)


#Build and store JamPerTrecho
geo_trechos = build_geo_trechos(meta)
jams_per_trecho = gpd.sjoin(geo_jams, geo_trechos, how="inner", op="contains")
jpt = meta.tables["JamPerTrecho"]
jpt.delete().execute()
jpt_tosql = prep_jpt_tosql(jams_per_trecho) 
jpt_tosql.to_sql("JamPerTrecho", con=meta.bind, if_exists="append", index=False)