import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import pandas as pd

from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy import ForeignKeyConstraint, UniqueConstraint, exc, ForeignKey, Float, Unicode
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import inspect, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import JSON as typeJSON
from sqlalchemy.types import BigInteger

from src.data.processing_func import (prep_section_tosql)

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
Base = declarative_base()
meta = MetaData()
meta.bind = engine
meta.reflect()

class Section(Base):
    __tablename__ = "Section"
    
    id = Column("SctnId", Integer, primary_key=True)
    arcgisId = Column("SctnIdArcgis", Integer)
    codLogr = Column("SctnCodRua", Integer)
    nomLogr = Column("SctnDscNome", Unicode)
    metrica = Column("SctnQtdMetrosAcumulados", Integer)
    comprimento = Column("SctnQtdComprimento", Float)
    xCom = Column("SctnDscCoordxUtmComeco", Float)
    yCom = Column("SctnDscCoordyUtmComeco", Float)
    xMeio = Column("SctnDscCoordxUtmMeio", Float)
    yMeio = Column("SctnDscCoordyUtmMeio", Float)
    xFinal = Column("SctnDscCoordxUtmFinal", Float)
    yFinal = Column("SctnDscCoordyUtmFinal", Float)

class MongoRecord(Base):
    __tablename__ = "MongoRecord"
    
    id = Column("MgrcId", Integer, primary_key=True)
    dateStart = Column("MgrcDateStart", DateTime(timezone=True))
    dateEnd = Column("MgrcDateEnd", DateTime(timezone=True))
    
    __table_args__ = (UniqueConstraint("MgrcDateStart", name="startDate_record"),
                      UniqueConstraint("MgrcDateEnd", name="endDate_record"),
                      {})

class Jam(Base):
    __tablename__ = "Jam"
    
    id = Column("JamId", Integer, primary_key=True)
    object_id = Column("JamObjectId", Unicode)
    dateStart = Column("JamDateStart", DateTime(timezone=True),
                       ForeignKey("MongoRecord.MgrcDateStart", ondelete="CASCADE"), nullable=False)
    dateEnd = Column("JamDateEnd", DateTime(timezone=True))
    city = Column("JamDscCity", Unicode)
    coords = Column("JamDscCoordinatesLonLat", typeJSON)
    roadType = Column("JamDscRoadType", Integer)
    segments = Column("JamDscSegments", typeJSON)
    street = Column("JamDscStreet", Unicode)
    endNode = Column("JamDscStreetEndNode", Unicode)
    turnType = Column("JamDscTurnType", Unicode)
    jam_type = Column("JamDscType", Unicode)
    level = Column("JamIndLevelOfTraffic", Integer)
    length = Column("JamQtdLengthMeters", Integer)
    speed = Column("JamSpdMetersPerSecond", Float)
    delay = Column("JamTimeDelayInSeconds", Integer)
    pubMillis = Column("JamTimePubMillis", BigInteger)
    uuid = Column("JamUuid", Integer)
    
    __table_args__ = (UniqueConstraint("JamDateStart", "JamUuid", name="JamDateUuid"),)

class JamPerSection(Base):
    __tablename__ = "JamPerSection"
    
    id = Column("JpsId", Integer, primary_key=True)
    JamDateStart = Column("JamDateStart", DateTime(timezone=True), nullable=False)
    JamUuid = Column("JamUuid", Integer, nullable=False) 
    SctnId = Column("SctnId", Integer, ForeignKey("Section.SctnId", ondelete="CASCADE"), nullable=False)
    
    __table_args__ = (ForeignKeyConstraint([JamDateStart, JamUuid],
                                           ["Jam.JamDateStart", "Jam.JamUuid"],
                                           ondelete="CASCADE"),
                      UniqueConstraint("JamDateStart", "JamUuid", "SctnId", name="jammed_section"),
                      {})

if 'Section' in meta.tables.keys():
    has_section = True
Base.metadata.create_all(engine)

if has_section == True:
    flush_sections = None
    while (flush_sections != "Y") and (flush_sections != "N") and (flush_sections != "y") and (flush_sections != "n"):
        flush_sections = input("""
        Do you wish to flush and recreate the Section table?
        This will cause you to lose any JamPerSection data you might have. (Y/N): """)

    if (flush_sections == "Y") or (flush_sections == "y"):
        prep_section_tosql(project_dir + "/data/external/sepud_logradouros.csv")
        df_sections.to_sql("Section", meta.bind, if_exists="append", index_label="SctnId")
    elif (flush_sections == "N") or (flush_sections == "n"):
        print("No changes applied to the Section table")