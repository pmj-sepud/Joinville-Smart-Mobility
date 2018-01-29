import time
import numpy as np
import pandas as pd
import geopandas as gpd
import numpy as np
from sqlalchemy import extract, select
from sqlalchemy.sql import or_, and_
import datetime
from shapely.geometry import Point

from src.data.processing_func import (get_direction, extract_geo_sections)

def extract_jps(meta, date_begin, date_end, periods=None, weekends=False, summary=False):
  start = time.time()

  jps = meta.tables["JamPerSection"]
  jam = meta.tables["Jam"]
  sctn = meta.tables["Section"]
  mongo_record = meta.tables["MongoRecord"]

  query = select([mongo_record.c.MgrcDateStart,
                  jps.c.JpsId,
                  jam.c.JamId,
                  jam.c.JamIndLevelOfTraffic,
                  jam.c.JamQtdLengthMeters,
                  jam.c.JamSpdMetersPerSecond,
                  jam.c.JamTimeDelayInSeconds,
                  jam.c.JamQtdLengthMeters,
                  jam.c.JamDscCoordinatesLonLat,
                  sctn.c.SctnId,
                  sctn.c.SctnDscNome,
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

  if periods:
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
  df_jps["date"] = pd.to_datetime(df_jps["MgrcDateStart"].dt.date)
  df_jps["hour"] = df_jps["MgrcDateStart"].dt.hour
  df_jps["minute"] = df_jps["MgrcDateStart"].dt.minute
  df_jps["period"] = np.sign(df_jps["hour"]-12)

  bins = [0, 14, 29, 44, 59]
  labels = []
  for i in range(1,len(bins)):
    if i==1:
      labels.append(str(bins[i-1]) + " a " + str(bins[i]))
    else:
      labels.append(str(bins[i-1]+1) + " a " + str(bins[i]))

  df_jps['minute_bin'] = pd.cut(df_jps["minute"], bins, labels=labels, include_lowest=True)
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

def transf_flow_features(df_jps):
  df_flow_features = df_jps.groupby(["SctnId", "date", "hour",
                   "minute_bin", "LonDirection", "LatDirection"]).agg(
                                                        {"JamQtdLengthMeters": ["mean"],
                                                         "JamSpdMetersPerSecond": ["mean"],
                                                         "JamTimeDelayInSeconds": ["mean"],
                                                         "JamIndLevelOfTraffic": ["mean"],
                                                        })
  df_flow_features.columns = ['_'.join(col).strip() for col in df_flow_features.columns.values]
  df_flow_features["JamSpdKmPerHour_mean"] = df_flow_features["JamSpdMetersPerSecond_mean"]*3.6
  columns = {"JamSpdKmPerHour_mean": "Velocidade Média (km/h)",
             "JamQtdLengthMeters_mean": "Fila média (m)",
             "JamTimeDelayInSeconds_mean": "Atraso médio (s)",
             "JamIndLevelOfTraffic_mean": "Nível médio de congestionamento (0 a 5)"
            }

  df_flow_features.rename(columns=columns, inplace=True)
  df_flow_features = df_flow_features[[col for col in columns.values()]]

  return df_flow_features

def transf_flow_labels(meta, path_fluxos):
  
  geo_sections = extract_geo_sections(meta)

  df_fluxos = pd.read_csv(path_fluxos, sep=';', decimal=',')
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
  df_flow_labels = gpd.sjoin(geo_fluxos, geo_sections, how="left", op="within")
  df_flow_labels["hour"] = df_flow_labels["Horario"].str[:2].astype(int)
  df_flow_labels["minute_bin"] = df_flow_labels["Horario"].str[3:5] + " a " + df_flow_labels["Horario"].str[12:14]
  df_flow_labels["minute_bin"] = df_flow_labels["minute_bin"].str.replace("00", "0")
  df_flow_labels.set_index(["SctnId", "date", "hour", "minute_bin", "Direction"], inplace=True)
  columns = ['Endereco', 'Corredor', 'Ciclofaixa', 'Numero de faixas', 'Sentido', 'Equipamento', '00 a 10',
             '11 a 20', '21 a 30', '31 a 40', '41 a 50', '51 a 60', '61 a 70',
             '71 a 80', '81 a 90', '91 a 100', 'Acima de 100', 'Total',
            ]
  df_flow_labels = df_flow_labels[columns]
  
  return df_flow_labels

def transf_traffic_per_timeslot(df_jps, meta, holiday_list):
  df_jps = df_jps[~df_jps["MgrcDateStart"].dt.date.isin(holiday_list)]
  wazesignals_per_timeslot = df_jps.groupby(["hour", "minute_bin"]).agg({"MgrcDateStart": pd.Series.nunique})

  jps_per_timeslot = df_jps.groupby(["SctnId", "hour",
                                     "minute_bin", "LonDirection","LatDirection"]) \
                                          .agg({"JpsId": ['count'],
                                               "JamQtdLengthMeters": ["mean"],
                                               "JamSpdMetersPerSecond": ["mean"],
                                               "JamTimeDelayInSeconds": ["mean"],
                                               "JamIndLevelOfTraffic": ["mean"],
                                               "period": ["max"],
                                               })
  
  jps_per_timeslot.reset_index(level=["SctnId", "LonDirection","LatDirection"], inplace=True)
  jps_per_timeslot.columns = [''.join(col_name).strip() for col_name in jps_per_timeslot.columns.values]
  jps_per_timeslot = jps_per_timeslot.join(wazesignals_per_timeslot, how="outer")
  jps_per_timeslot["JamSpdKmPerHourmean"] = jps_per_timeslot["JamSpdMetersPerSecondmean"]*3.6
  jps_per_timeslot["traffic_prob"] = jps_per_timeslot["JpsIdcount"]/jps_per_timeslot["MgrcDateStart"]

  columns = {"MgrcDateStart": "Total de sinais do Waze",
             "JpsIdcount": "Engarrafamentos registrados",
             "traffic_prob":"traffic_prob",
             "JamSpdKmPerHourmean": "Velocidade Média (km/h)",
             "JamQtdLengthMetersmean": "Fila média (m)",
             "JamTimeDelayInSecondsmean": "Atraso médio (s)",
             "JamIndLevelOfTrafficmean": "Nível médio de congestionamento (0 a 5)",
             "periodmax": "period",
            }
  jps_per_timeslot.rename(columns=columns, inplace=True)

  geo_sections = extract_geo_sections(meta, buffer=8)
  jps_per_timeslot.reset_index(inplace=True)
  geo_jps_per_timeslot = geo_sections.merge(jps_per_timeslot, how="inner", on="SctnId")
  geo_jps_per_timeslot.set_index(["SctnId", "SctnDscNome", "LonDirection","LatDirection", "hour", "minute_bin"], inplace=True)

  col_list = [col for col in columns.values()]
  col_list.append("section_LineString")
  geo_jps_per_timeslot = geo_jps_per_timeslot[col_list]


  return geo_jps_per_timeslot

def transf_probability_matrix(geo_jps_per_timeslot, sections_interest):
  sections_interest.columns = sections_interest.columns.str.strip() 
  sections_interest["geometry"] = sections_interest.apply(
                                        lambda row: Point(row["Longitude"], row["Latitude"]), axis=1)
  crs = geo_jps_per_timeslot.crs
  geo_sections_interest = gpd.GeoDataFrame(sections_interest, crs=crs, geometry="geometry")
  prob_matrix = gpd.sjoin(geo_sections_interest, geo_jps_per_timeslot, how="left", op="within")

  return prob_matrix

def gen_traffic_indicators(prob_matrix):
  prob_matrix["notraffic_prob"] = 1 - prob_matrix["traffic_prob"]
  prob_matrix["prod_Velocidade Média (km/h)"] = prob_matrix["traffic_prob"]*prob_matrix["Velocidade Média (km/h)"]
  prob_matrix["prod_Fila média (m)"] = prob_matrix["traffic_prob"]*prob_matrix["Fila média (m)"]
  prob_matrix["prod_Atraso médio (s)"] = prob_matrix["traffic_prob"]*prob_matrix["Atraso médio (s)"]
  prob_matrix["prod_Nível médio de congestionamento (0 a 5)"] = prob_matrix["traffic_prob"]*prob_matrix["Nível médio de congestionamento (0 a 5)"]

  g = prob_matrix.groupby(["SctnId", "SctnDscNome", "Longitude", "Latitude", "LonDirection", "LatDirection", "period"]).agg({'notraffic_prob': np.prod,
                                                                          'traffic_prob': np.sum,
                                                                           "prod_Velocidade Média (km/h)": np.sum,
                                                                           "prod_Fila média (m)": np.sum,
                                                                           "prod_Atraso médio (s)": np.sum,
                                                                           "prod_Nível médio de congestionamento (0 a 5)": np.sum}) 
  
  g["Probabilidade de Trânsito"] = 1 - g["notraffic_prob"]
  g["Velocidade Média (km/h)"] = g["prod_Velocidade Média (km/h)"] / g["traffic_prob"]
  g["Fila média (m)"] = g["prod_Fila média (m)"] / g["traffic_prob"]
  g["Atraso médio (s)"] = g["prod_Atraso médio (s)"] / g["traffic_prob"]
  g["Nível médio de congestionamento (0 a 5)"] = g["prod_Nível médio de congestionamento (0 a 5)"] / g["traffic_prob"]



  g = g[["Probabilidade de Trânsito",
         "Velocidade Média (km/h)",
         "Fila média (m)",
         "Atraso médio (s)",
         "Nível médio de congestionamento (0 a 5)"
         ]
      ]

  return g