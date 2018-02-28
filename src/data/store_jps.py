import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy import MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import TIMESTAMP as typeTIMESTAMP

from src.data.processing_func import (extract_geo_jams, extract_geo_sections, store_jps, connect_database)

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

meta = connect_database(DATABASE)

#Flush JamPersection and Build geo_sections
flush = None
while (flush != "Y") and (flush != "N") and (flush != "y") and (flush != "n"):
    flush = input("Are you sure you want to flush JamPerSection table? (Y/N): ")
    if (flush == "Y") or (flush == "y"):
        print("Flushing JamPerSection...")
        jps = meta.tables["JamPerSection"]
        jps.delete().execute()
    elif (flush == "N") or (flush == "n"):
        print("Aborting operation...")
        sys.exit()

store_jps(meta)