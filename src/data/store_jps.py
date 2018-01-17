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

from src.data.processing_func import (build_geo_jams, build_geo_sections)

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

#Build geo_jams and geo_sections
geo_jams = build_geo_jams(meta)
geo_sections = build_geo_sections(meta)

#Build and store JamPerSection
jams_per_section = gpd.sjoin(geo_jams, geo_sections, how="inner", op="contains")
jams_per_section = jams_per_section[["JamDateStart", "JamUuid", "SctnId"]]
jpt = meta.tables["JamPerSection"]
jpt.delete().execute()
jams_per_section.to_sql("JamPerSection", con=meta.bind, if_exists="append", index=False)