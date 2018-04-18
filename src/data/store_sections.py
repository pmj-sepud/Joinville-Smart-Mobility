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

from src.data.processing_func import (prep_section_tosql, connect_database)

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
meta.reflect(schema="geo")
sections = meta.tables["geo.sections"]

flush_sections = None
while (flush_sections != "Y") and (flush_sections != "N") and (flush_sections != "y") and (flush_sections != "n"):
    flush_sections = input("Do you wish to flush and recreate the Section table (Y/N): ")

if (flush_sections == "Y") or (flush_sections == "y"):
    sections.delete().execute()
    wkt_file_path = project_dir + "/data/external/sepud_logradouros_WKT_abr2018.csv"
    df_sections = prep_section_tosql(wkt_file_path)
    df_sections.to_sql("sections", meta.bind, schema="geo", if_exists="append", index=False)
elif (flush_sections == "N") or (flush_sections == "n"):
    print("No changes applied to the Section table")