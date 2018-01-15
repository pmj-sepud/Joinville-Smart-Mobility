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
Base = declarative_base()
meta = MetaData()
meta.bind = engine
meta.reflect()

class Trecho(Base):
    __tablename__ = "Trecho"
    
    id = Column("TrchId", Integer, primary_key=True)
    arcgisId = Column("TrchIdArcgis", Integer)
    codLogr = Column("TrchCodRua", Integer)
    nomLogr = Column("TrchDscNome", Unicode)
    metrica = Column("TrchQtdMetrosAcumulados", Integer)
    comprimento = Column("TrchQtdComprimento", Float)
    xCom = Column("TrchDscCoordxUtmComeco", Float)
    yCom = Column("TrchDscCoordyUtmComeco", Float)
    xMeio = Column("TrchDscCoordxUtmMeio", Float)
    yMeio = Column("TrchDscCoordyUtmMeio", Float)
    xFinal = Column("TrchDscCoordxUtmFinal", Float)
    yFinal = Column("TrchDscCoordyUtmFinal", Float)

class MongoRecord(Base):
    __tablename__ = "MongoRecord"
    
    id = Column("MgrcId", Integer, primary_key=True)
    dateStart = Column("MgrcDateStart", DateTime)
    dateEnd = Column("MgrcDateEnd", DateTime)
    
    __table_args__ = (UniqueConstraint("MgrcDateStart", name="startDate_record"),
                      UniqueConstraint("MgrcDateEnd", name="endDate_record"),
                      {})

class Jam(Base):
    __tablename__ = "Jam"
    
    id = Column("JamId", Integer, primary_key=True)
    object_id = Column("JamObjectId", Unicode)
    dateStart = Column("JamDateStart", DateTime,
                       ForeignKey("MongoRecord.MgrcDateStart", ondelete="CASCADE"), nullable=False)
    dateEnd = Column("JamDateEnd", DateTime)
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

class JamPerTrecho(Base):
    __tablename__ = "JamPerTrecho"
    
    id = Column("JptId", Integer, primary_key=True)
    JamDateStart = Column("JamDateStart", DateTime, nullable=False)
    JamUuid = Column("JamUuid", Integer, nullable=False) 
    TrchId = Column("TrchId", Integer, ForeignKey("Trecho.TrchId"), nullable=False)
    
    __table_args__ = (ForeignKeyConstraint([JamDateStart, JamUuid],
                                           ["Jam.JamDateStart", "Jam.JamUuid"],
                                           ondelete="CASCADE"),
                      UniqueConstraint("JamDateStart", "JamUuid", "TrchId", name="trecho_engarrafado"),
                      {})

Base.metadata.create_all(engine)

df_trechos = pd.read_csv(project_dir + "/data/external/sepud_logradouros.csv", decimal=",")

columns = {"objectid,N,10,0": "TrchIdArcgis",
          "codlogra,N,10,0": "TrchCodRua",
          "nomelog,C,254": "TrchDscNome",
          "acumulo,N,10,0": "TrchQtdMetrosAcumulados",
          "st_length_,N,19,11": "TrchQtdComprimento",
          "Coord_x,N,19,11": "TrchDscCoordxUtmComeco",
          "coord_y,N,19,11": "TrchDscCoordyUtmComeco",
          "Cood_x_m,N,19,11": "TrchDscCoordxUtmMeio",
          "Coord_y_m,N,19,11": "TrchDscCoordyUtmMeio",
          "coord_x_f,N,19,11": "TrchDscCoordxUtmFinal",
          "coord_y_f,N,19,11": "TrchDscCoordyUtmFinal",
          }

df_trechos.rename(columns=columns, inplace=True)

cols = [v for k, v in columns.items() ]
df_trechos = df_trechos[cols]

#Pedir confirmação do usuário antes de implementar a função
trecho = meta.tables["Trecho"]
trecho.delete().execute()
df_trechos.to_sql("Trecho", meta.bind, if_exists="append", index_label="TrchId")