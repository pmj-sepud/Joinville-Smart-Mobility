import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import extract, select
from sqlalchemy.sql import or_, and_
import datetime

from src.data.processing_func import (get_direction)

dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)


def gen_df_jps(meta, date_begin, date_end, periods=None, weekends=False, summary=False):
  start = time.time()

  jps = meta.tables["JamPerSection"]
  jam = meta.tables["Jam"]
  sctn = meta.tables["Section"]
  mongo_record = meta.tables["MongoRecord"]

  query = select([mongo_record.c.MgrcDateStart,
                  jps.c.JpsId,
                  jps.c.JamUuid,
                  jam.c.JamId,
                  jam.c.JamIndLevelOfTraffic,
                  jam.c.JamQtdLengthMeters,
                  jam.c.JamSpdMetersPerSecond,
                  jam.c.JamTimeDelayInSeconds,
                  jam.c.JamQtdLengthMeters,
                  jam.c.JamDscCoordinatesLonLat,
                  sctn.c.SctnId,
                  sctn.c.SctnDscNome,
                  sctn.c.SctnQtdComprimento,
                  sctn.c.SctnDscCoordxUtmComeco,
                  sctn.c.SctnDscCoordyUtmComeco,
                  sctn.c.SctnDscCoordxUtmMeio,
                  sctn.c.SctnDscCoordyUtmMeio,
                  sctn.c.SctnDscCoordxUtmFinal,
                  sctn.c.SctnDscCoordyUtmFinal]).\
                  select_from(mongo_record.join(jam.join(jps).join(sctn), isouter=True)).\
                  where(mongo_record.c.MgrcDateStart.between(date_begin, date_end))

  if not weekends:
    query = query.where(extract("isodow", mongo_record.c.MgrcDateStart).in_(list(range(1,5))))

  or_list=[]
  for t in periods:
    or_list.append(and_(extract("hour", mongo_record.c.MgrcDateStart)>=t[0],
                        extract("hour", mongo_record.c.MgrcDateStart)<t[1]
                        )
                  )
  query = query.where(or_(*or_list))
  df_jps = pd.read_sql(query, meta.bind)  
  df_jps[["LonDirection","LatDirection"]] = df_jps["JamDscCoordinatesLonLat"].apply(get_direction)
  df_jps["MgrcDateStart"] = df_jps["MgrcDateStart"].dt.tz_convert("America/Sao_Paulo")
  end = time.time()

  processing_time = round(end - start)

  if summary:
    minutos_engarrafados = df_jps["JamId"].nunique()
    n_ruas = df_jps["SctnDscNome"].nunique()
    n_trechos = df_jps["SctnId"].nunique()

    print("Tempo para carregamento dos dados: " + str(processing_time) + " segundos.")
    print("Minutos de engarrafamento carregados: " + str(minutos_engarrafados))
    print("Número de ruas abrangidas: " + str(n_ruas))
    print("Número de trechos abrangidos: " + str(n_trechos))
    columns = {"SctnDscNome": "Rua",
               "SctnId": "Section",
               "MgrcDateStart": "Data (Horario Brasília)",
               "JamQtdLengthMeters": "Comprimento da fila (m)",
               "JamSpdMetersPerSecond": "Velocidade (km/h)",
               "JamTimeDelayInSeconds": "Atraso (s)",
               "JamIndLevelOfTraffic": "Nível de trânsito (0 a 5)",
              }

    df_jps_toshow = df_jps.rename(columns=columns)
    df_jps_toshow["Velocidade (km/h)"] = df_jps_toshow["Velocidade (km/h)"]*3.6
    df_jps_toshow[[c for c in columns.values()]].sample(7).sort_values("Data (Horario Brasília)", ascending=False)

  return df_jps

def gen_df_features(df_jps):
  df["date"] = df["JamDateStart"].dt.date
  df["hour"] = df["JamDateStart"].dt.hour-2
  #df_jpt_trecho["hour"] = df_jpt_trecho.apply(lambda x: aplicar_horario_verao(x["date"], x["hour"]), axis=1)
  df["minute"] = df["JamDateStart"].dt.minute
  df["period"] = np.sign(df["hour"]-12)
  df = df[~df["JamDateStart"].dt.date.isin(feriados)]