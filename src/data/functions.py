import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from src.common import exceptions
from pyproj import Proj
import geojson
from pymongo import MongoClient, DESCENDING

def collect_records(collection, limit=None):
    
    if limit:
        records = list(collection.find(sort=[("_id", DESCENDING)]).limit(limit))
    else:
        records = list(collection.find(sort=[("_id", DESCENDING)]))

    return records

def tabulate_records(records):
        
    raw_data = pd.DataFrame(records)
    raw_data['startTime'] = pd.to_datetime(raw_data['startTime'].str[:-4])
    raw_data['endTime'] = pd.to_datetime(raw_data['endTime'].str[:-4])

    return raw_data

def prep_rawdata_tosql(raw_data):

    raw_data_tosql = raw_data[["startTime", "endTime"]]
    rename_dict = {"startTime": "MgrcDateStart",
                   "endTime": "MgrcDateEnd",
                  }

    raw_data_tosql = raw_data_tosql.rename(columns=rename_dict)

    return raw_data_tosql

def json_to_df(row, json_column):
    df_from_json = pd.io.json.json_normalize(row[json_column]).add_prefix(json_column + '_')    
    df = pd.concat([row]*len(df_from_json), axis=1).transpose()    
    df.reset_index(inplace=True, drop=True)    
    
    return pd.concat([df, df_from_json], axis=1)

def lon_lat_to_UTM(l):
    '''
    Convert list of Lat/Lon to UTM coordinates
    '''
    proj = Proj("+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
    list_of_coordinates = []
    for t in l:
        lon, lat = t
        X, Y = proj(lon,lat)
        list_of_coordinates.append(tuple([X, Y]))
        
    return list_of_coordinates

def UTM_to_lon_lat(l):
    '''
    Convert df_jams from UTM coordinates to Lat/Lon
    '''
    proj = Proj("+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs")
    list_of_coordinates = []
    for t in l:
        X, Y = t
        lon, lat = proj(X,Y, inverse=True)
        list_of_coordinates.append(tuple([lon, lat]))
        
    return list_of_coordinates

def build_df_alerts(raw_data):
    if 'alerts' in raw_data:
        df_alerts_cleaned = raw_data[~(raw_data['alerts'].isnull())]
        df_alerts = pd.concat([json_to_df(row, 'alerts') for _, row in df_alerts_cleaned.iterrows()])
        df_alerts.reset_index(inplace=True, drop=True)
    else:
        raise Exception("No Alerts in the given period")
        
    return df_alerts

def build_df_alerts(raw_data):
    if 'alerts' in raw_data:
        df_alerts_cleaned = raw_data[~(raw_data['alerts'].isnull())]
        df_alerts = pd.concat([json_to_df(row, 'alerts') for _, row in df_alerts_cleaned.iterrows()])
        df_alerts.reset_index(inplace=True, drop=True)
    else:
        raise Exception("No Alerts in the given period")
        
    return df_alerts

def build_df_jams(raw_data):
    if 'jams' in raw_data:
        df_jams_cleaned = raw_data[~(raw_data['jams'].isnull())]
        df_jams = pd.concat([json_to_df(row, 'jams') for _, row in df_jams_cleaned.iterrows()])
        df_jams.reset_index(inplace=True, drop=True)
        df_jams['jams_line_list'] = df_jams['jams_line'].apply(lambda x: [tuple([d['x'], d['y']]) for d in x])
        df_jams['jams_line_UTM'] = df_jams['jams_line_list'].apply(lon_lat_to_UTM)
        df_jams['jam_LineString'] = df_jams.apply(lambda x: LineString(x['jams_line_UTM']).buffer(12), axis=1)
    else:
        raise exceptions.NoJamError()
        
    return df_jams
    
def build_df_irregularities(raw_data):
    if 'irregularities' in raw_data:
        df_irregularities_cleaned = raw_data[~(raw_data['irregularities'].isnull())]
        df_irregularities = pd.concat([json_to_df(row, 'irregularities') for _, row in df_irregularities_cleaned.iterrows()])
        df_irregularities.reset_index(inplace=True, drop=True)
    else:
        raise Exception("No Irregularities in the given period")
        
    return df_irregularities

def get_impacted_trechos(row, df_trechos):
    df_trechos = df_trechos[df_trechos["TrchDscNome"]==row["LwsDscSepudStreet"]].copy()
    df_trechos["Traffic?"] = df_trechos['trecho_LineString'].apply(lambda x: x.intersects(row['jam_LineString']))
    trch_list = df_trechos[df_trechos["Traffic?"]==True].index.tolist()
    
    return trch_list

def explode_impacted_trechos(df_jams):
    rows = []  
    def append_twice(row, logr):
        temp_list = row.drop("impacted_trechos").tolist()
        temp_list.append(logr)
        rows.append(temp_list)

    _ = df_jams.copy().apply(lambda row: [append_twice(row, logr) for logr in row['impacted_trechos']], axis=1)
    
    df_jams_pertrecho = pd.DataFrame(rows, columns=df_jams.columns)
    
    return df_jams_pertrecho

def print_coordinates(coords):

    l = []
    for t in coords:
        x, y = t
        l.append({'lat': y, 'lng': x})
        
    string = ''
    for coord in l:
        pair = ",".join("{}: {}".format(k,v) for k,v in coord.items())
        pair = "{" + pair + "},"
        string += pair
    string = string.strip()

    return string

def df_to_geojson(df, filename="result_geojson.json"):
    features = []
    df.apply(lambda x: features.append(
        geojson.Feature(geometry=geojson.LineString(x["Street_line_LonLat"]),
                        properties={"id": int(x.name),
                                   "rua": x["Rua"],
                                   "nivel_medio": str(x["Nivel médio (0 a 5)"]),
                                   "velocidade_media": str(x["Velocidade média (km/h)"]),
                                   "percentual_transito": str(x["Percentual de trânsito (min engarrafados / min monitorados)"]),
                                   "comprimento": x["Comprimento (m)"],
                                   "atraso_medio": x["Atraso médio (s)"],
                                   "atraso_por_metro": x["Atraso por metro (s/m)"]
                                  }
                      )
        ), axis=1)
    
    with open(filename, "w") as fp:
        geojson.dump(geojson.FeatureCollection(features), fp, sort_keys=True)

def normalize_jpt(df_jpt):
    #Avoid double-counting of jams (probably lanes)
    norm_df_jpt = pd.pivot_table(df_jpt,
                                 index=["TrchId",
                                        "JamDateStart",
                                        "TrchDscNome",
                                        "TrchQtdComprimento",
                                        "TrchDscCoordxUtmComeco",
                                        "TrchDscCoordyUtmComeco",
                                        "ClfuDscClassFunc",
                                        "LonDirection",
                                        "LatDirection"])
    norm_df_jpt.reset_index(inplace=True)
    norm_df_jpt.drop("JamUuid", axis=1, inplace=True)
    
    return norm_df_jpt

def get_pivot_jpt_means(norm_df_jpt):
    #Médias
    pivot_jpt_means = pd.pivot_table(norm_df_jpt,
                                     index=["TrchId", "TrchDscNome", "TrchDscCoordxUtmComeco", "TrchDscCoordyUtmComeco", "ClfuDscClassFunc"],
                                     values=["JamIndLevelOfTraffic",
                                             "JamIndLevelOfTrafficSqrd",
                                             "JamSpdMetersPerSecond",
                                             "TrchQtdComprimento",
                                             "JamTimeDelayInSeconds",
                                             "JamQtdLengthMeters"]
                                    )
    
    pivot_jpt_means['JamSpdKmPerHour'] = pivot_jpt_means['JamSpdMetersPerSecond']*3.6
    
    return pivot_jpt_means

def get_pivot_jpt_count(norm_df_jpt):
    #Contagens
    pivot_jpt_count = pd.pivot_table(norm_df_jpt,
                                     index=["TrchId", "TrchDscNome", "TrchDscCoordxUtmComeco", "TrchDscCoordyUtmComeco", "ClfuDscClassFunc"],
                                     values=["JamDateStart"],
                                     aggfunc= "count"                
                                     )
    return pivot_jpt_count

def gen_pivot_table(df_jpt, total_observations):
    
    norm_df_jpt = normalize_jpt(df_jpt)
    pivot_jpt_means = get_pivot_jpt_means(norm_df_jpt)
    pivot_jpt_count = get_pivot_jpt_count(norm_df_jpt)
    
    #Concatenate and refine
    pivot_jpt = pd.concat([pivot_jpt_means, pivot_jpt_count], axis=1)
    pivot_jpt['Percentual de trânsito (min engarrafados / min monitorados)'] = pivot_jpt['JamDateStart'] / total_observations
    pivot_jpt.reset_index(inplace=True)
    pivot_jpt.set_index("TrchId", inplace=True)
    pivot_jpt.drop(["JamDateStart", "JamSpdMetersPerSecond"], axis=1, inplace=True)
    pivot_jpt.rename(columns={'JamSpdKmPerHour': 'Velocidade média (km/h)',
                              'JamIndLevelOfTraffic': 'Nivel médio (0 a 5)',
                              'JamIndLevelOfTrafficSqrd': 'Nivel quadrático médio (0 a 25)',
                              'TrchQtdComprimento': "Comprimento do trecho (m)",
                              'JamQtdLengthMeters': "Comprimento médio de fila (m)",
                              'JamTimeDelayInSeconds': "Atraso médio (s)",
                              'TrchDscNome': "Rua",
                              'ClfuDscClassFunc': "Classificação Funcional"}, inplace=True)
    #Show
    cols = ["Rua",
            "Percentual de trânsito (min engarrafados / min monitorados)",
            "Comprimento médio de fila (m)",
            "Atraso médio (s)",
            "Nivel médio (0 a 5)",
            "Nivel quadrático médio (0 a 25)",
            "Velocidade média (km/h)",
            "Comprimento do trecho (m)",
            "TrchDscCoordxUtmComeco",
            "TrchDscCoordyUtmComeco",
            "Classificação Funcional"]
    pivot_jpt = pivot_jpt[cols]
    pivot_jpt = pivot_jpt.sort_values("Percentual de trânsito (min engarrafados / min monitorados)", ascending=False)
    
    return pivot_jpt

def get_direction(coord_list):
    num_coords = len(coord_list)
    
    #North/South
    y_start = coord_list[0]["y"]
    y_end = coord_list[num_coords-1]["y"]
    if (y_end-y_start) >= 0:
        lat_direction = "Norte"
    else:
        lat_direction = "Sul"
        
    #East/West
    x_start = coord_list[0]["x"]
    x_end = coord_list[num_coords-1]["x"]
    if (x_end-x_start) >= 0:
        lon_direction = "Leste"
    else:
        lon_direction = "Oeste"
        
    return pd.Series([lon_direction, lat_direction])