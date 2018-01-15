import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import geopandas as gpd

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker

from src.data.functions import (build_geo_jams, build_geo_trechos)

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
meta = MetaData()
meta.bind = engine
meta.reflect()

#Build geo_jams and geo_trechos
geo_jams = build_geo_jams(meta)
geo_trechos = build_geo_trechos(meta)

#Build and store JamPerTrecho
jams_per_trecho = gpd.sjoin(geo_jams, geo_trechos, how="inner", op="contains")
jams_per_trecho = jams_per_trecho[["JamDateStart", "JamUuid", "TrchId"]]
jpt = meta.tables["JamPerTrecho"]
jpt.delete().execute()
jams_per_trecho.to_sql("JamPerTrecho", con=meta.bind, if_exists="append", index=False)