import time
import pandas as pd
import geopandas as gpd
import numpy as np
from sqlalchemy import extract, select
from sqlalchemy.sql import or_, and_
import datetime
from shapely.geometry import Point

from src.data.processing_func import (get_direction, build_geo_sections)

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

def gen_df_traffic(df):
  df["date"] = pd.to_datetime(df["MgrcDateStart"].dt.date)
  df["hour"] = df["MgrcDateStart"].dt.hour
  df["minute"] = df["MgrcDateStart"].dt.minute
  df["period"] = np.sign(df["hour"]-12)

  bins = [0, 14, 29, 44, 59]
  labels = []
  for i in range(1,len(bins)):
    if i==1:
      labels.append(str(bins[i-1]) + " a " + str(bins[i]))
    else:
      labels.append(str(bins[i-1]+1) + " a " + str(bins[i]))

  df['minute_bin'] = pd.cut(df["minute"], bins, labels=labels, include_lowest=True)

  gb = df.groupby(["SctnId", "date", "hour",
                   "minute_bin", "LonDirection", "LatDirection"]).agg(
                                                        {"MgrcDateStart": ['count'],
                                                         "JpsId": ['count'],
                                                         "JamQtdLengthMeters": ["mean"],
                                                         "JamSpdMetersPerSecond": ["mean"],
                                                         "JamTimeDelayInSeconds": ["mean"],
                                                         "JamIndLevelOfTraffic": ["mean"],
                                                        })
  gb.columns = ['_'.join(col).strip() for col in gb.columns.values]
  gb["JamSpdKmPerHour_mean"] = gb["JamSpdMetersPerSecond_mean"]*3.6
  gb["Percentual de trânsito (min engarrafados / min monitorados)"] = gb["JpsId_count"] / gb["MgrcDateStart_count"]
  colunas = {"MgrcDateStart_count": "Total de sinais do Waze",
             "JpsId_count": "Engarrafamentos registrados",
             "Percentual de trânsito (min engarrafados / min monitorados)":"Percentual de trânsito (min engarrafados / min monitorados)",
             "JamSpdKmPerHour_mean": "Velocidade Média (km/h)",
             "JamQtdLengthMeters_mean": "Fila média (m)",
             "JamTimeDelayInSeconds_mean": "Atraso médio (s)",
             "JamIndLevelOfTraffic_mean": "Nível médio de congestionamento (0 a 5)"
            }

  gb.rename(columns=colunas, inplace=True)
  gb = gb[[col for col in colunas.values()]]

  return gb

def gen_df_fluxos(meta, path_fluxos):
  
  geo_sections = build_geo_sections(meta)

  df_fluxos = pd.read_excel(path_fluxos)
  df_fluxos.dropna(subset=["Latitude", "Longitude"], inplace=True)
  df_fluxos["fluxo_Point"] = df_fluxos.apply(lambda x: Point(x["Longitude"], x["Latitude"]), axis=1)
  direction = {"N": "North",
            "S": "South",
            "Norte": "North",
            "Sul": "South",
            "L": "East",
            "O": "West",
            "Leste": "East",
            "Oeste": "West",}
  df_fluxos["Direction"] = df_fluxos["Sentido"].str.split("/", 1).str.get(1).map(direction)
  df_fluxos["date"] = pd.to_datetime(df_fluxos["Data"], dayfirst=True)
  
  geo_fluxos = gpd.GeoDataFrame(df_fluxos, crs={'init': 'epsg:4326'}, geometry="fluxo_Point")
  geo_fluxos = gpd.sjoin(geo_fluxos, geo_sections, how="left", op="within")
  geo_fluxos["hour"] = geo_fluxos["Horario"].str[:2].astype(int)
  geo_fluxos["minute_bin"] = geo_fluxos["Horario"].str[3:5] + " a " + geo_fluxos["Horario"].str[12:14]
  geo_fluxos["minute_bin"] = geo_fluxos["minute_bin"].str.replace("00", "0")
  geo_fluxos.set_index(["SctnId", "date", "hour", "minute_bin", "Direction"], inplace=True)
  columns = ['Endereco', 'Sentido', 'Equipamento', '00 a 10',
             '11 a 20', '21 a 30', '31 a 40', '41 a 50', '51 a 60', '61 a 70',
             '71 a 80', '81 a 90', '91 a 100', 'Acima de 100', 'Total',
            ]
  geo_fluxos = geo_fluxos[columns]
  
  return geo_fluxos