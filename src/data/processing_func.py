import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from pyproj import Proj
import geojson
from pymongo import MongoClient, DESCENDING
from shapely.geometry import LineString
import geopandas as gpd

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

def tabulate_jams(raw_data):
    if 'jams' in raw_data:
        df_jams_cleaned = raw_data[~(raw_data['jams'].isnull())]
        df_jams = pd.concat([json_to_df(row, 'jams') for _, row in df_jams_cleaned.iterrows()])
        df_jams.reset_index(inplace=True, drop=True)
    else:
        raise Exception()
        
    return df_jams


def tabulate_alerts(raw_data):
    if 'alerts' in raw_data:
        df_alerts_cleaned = raw_data[~(raw_data['alerts'].isnull())]
        df_alerts = pd.concat([json_to_df(row, 'alerts') for _, row in df_alerts_cleaned.iterrows()])
        df_alerts.reset_index(inplace=True, drop=True)
    else:
        raise Exception("No Alerts in the given period")
        
    return df_alerts

    
def tabulate_irregularities(raw_data):
    if 'irregularities' in raw_data:
        df_irregularities_cleaned = raw_data[~(raw_data['irregularities'].isnull())]
        df_irregularities = pd.concat([json_to_df(row, 'irregularities') for _, row in df_irregularities_cleaned.iterrows()])
        df_irregularities.reset_index(inplace=True, drop=True)
    else:
        raise Exception("No Irregularities in the given period")
        
    return df_irregularities

def prep_jams_tosql(df_jams):
    rename_dict = {"_id": "JamObjectId",
                   "endTime": "JamDateEnd",
                   "startTime": "JamDateStart",
                   "jams_city": "JamDscCity",
                   "jams_delay": "JamTimeDelayInSeconds",
                   "jams_endNode": "JamDscStreetEndNode",
                   "jams_length": "JamQtdLengthMeters",
                   "jams_level": "JamIndLevelOfTraffic",
                   "jams_pubMillis": "JamTimePubMillis",
                   "jams_roadType": "JamDscRoadType",
                   "jams_segments": "JamDscSegments",
                   "jams_speed": "JamSpdMetersPerSecond",
                   "jams_street": "JamDscStreet",
                   "jams_turnType": "JamDscTurnType",
                   "jams_type": "JamDscType",
                   "jams_uuid": "JamUuid",
                   "jams_line": "JamDscCoordinatesLonLat",
                  }

    col_list = list(rename_dict.values())
    jams_tosql = df_jams.rename(columns=rename_dict)
    jams_tosql["JamObjectId"] = jams_tosql["JamObjectId"].astype(str)

    actual_col_list = list(set(list(jams_tosql)).intersection(set(col_list)))
    jams_tosql = jams_tosql[actual_col_list]

    return jams_tosql


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

def build_geo_sections(meta, buffer=10):
    section = meta.tables['Section']
    sections_query = section.select()
    df_sections = pd.read_sql(sections_query, con=meta.bind)

    #Create Geometry shapes
    df_sections["Street_line_XY"] = df_sections.apply(lambda x: [tuple([x['SctnDscCoordxUtmComeco'], x['SctnDscCoordyUtmComeco']]),
                                                               tuple([x['SctnDscCoordxUtmMeio'], x['SctnDscCoordyUtmMeio']]),
                                                               tuple([x['SctnDscCoordxUtmFinal'], x['SctnDscCoordyUtmFinal']]),
                                                              ], axis=1)

    df_sections["Street_line_LonLat"] = df_sections['Street_line_XY'].apply(UTM_to_lon_lat)
    df_sections['section_LineString'] = df_sections.apply(lambda x: LineString(x['Street_line_XY']).buffer(buffer), axis=1)
    crs = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    geo_sections = gpd.GeoDataFrame(df_sections, crs=crs, geometry="section_LineString")
    geo_sections = geo_sections.to_crs({'init': 'epsg:4326'})

    return geo_sections

def build_geo_jams(meta, buffer=20):
    jam = meta.tables['Jam']
    jams_query = jam.select()
    df_jams = pd.read_sql(jams_query, con=meta.bind)
    df_jams['jams_line_list'] = df_jams['JamDscCoordinatesLonLat'].apply(lambda x: [tuple([d['x'], d['y']]) for d in x])
    df_jams['jams_line_UTM'] = df_jams['jams_line_list'].apply(lon_lat_to_UTM)
    df_jams['jam_LineString'] = df_jams.apply(lambda x: LineString(x['jams_line_UTM']).buffer(buffer), axis=1)
    crs = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    geo_jams = gpd.GeoDataFrame(df_jams, crs=crs, geometry="jam_LineString")
    geo_jams = geo_jams.to_crs({'init': 'epsg:4326'})

    return geo_jams

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

def normalize_jps(df_jps):
    #Avoid double-counting of jams (probably lanes)
    norm_df_jps = pd.pivot_table(df_jps,
                                 index=["SctnId",
                                        "JamDateStart",
                                        "SctnDscNome",
                                        "SctnQtdComprimento",
                                        "SctnDscCoordxUtmComeco",
                                        "SctnDscCoordyUtmComeco",
                                        "ClfuDscClassFunc",
                                        "LonDirection",
                                        "LatDirection"])
    norm_df_jps.reset_index(inplace=True)
    norm_df_jps.drop("JamUuid", axis=1, inplace=True)
    
    return norm_df_jps

def get_pivot_jps_means(norm_df_jps):
    #Médias
    pivot_jps_means = pd.pivot_table(norm_df_jps,
                                     index=["SctnId", "SctnDscNome", "SctnDscCoordxUtmComeco", "SctnDscCoordyUtmComeco", "ClfuDscClassFunc"],
                                     values=["JamIndLevelOfTraffic",
                                             "JamIndLevelOfTrafficSqrd",
                                             "JamSpdMetersPerSecond",
                                             "SctnQtdComprimento",
                                             "JamTimeDelayInSeconds",
                                             "JamQtdLengthMeters"]
                                    )
    
    pivot_jps_means['JamSpdKmPerHour'] = pivot_jps_means['JamSpdMetersPerSecond']*3.6
    
    return pivot_jps_means

def get_pivot_jps_count(norm_df_jps):
    #Contagens
    pivot_jps_count = pd.pivot_table(norm_df_jps,
                                     index=["SctnId", "SctnDscNome", "SctnDscCoordxUtmComeco", "SctnDscCoordyUtmComeco", "ClfuDscClassFunc"],
                                     values=["JamDateStart"],
                                     aggfunc= "count"                
                                     )
    return pivot_jps_count

def gen_pivot_table(df_jps, total_observations):
    
    norm_df_jps = normalize_jps(df_jps)
    pivot_jps_means = get_pivot_jps_means(norm_df_jps)
    pivot_jps_count = get_pivot_jps_count(norm_df_jps)
    
    #Concatenate and refine
    pivot_jps = pd.concat([pivot_jps_means, pivot_jps_count], axis=1)
    pivot_jps['Percentual de trânsito (min engarrafados / min monitorados)'] = pivot_jps['JamDateStart'] / total_observations
    pivot_jps.reset_index(inplace=True)
    pivot_jps.set_index("SctnId", inplace=True)
    pivot_jps.drop(["JamDateStart", "JamSpdMetersPerSecond"], axis=1, inplace=True)
    pivot_jps.rename(columns={'JamSpdKmPerHour': 'Velocidade média (km/h)',
                              'JamIndLevelOfTraffic': 'Nivel médio (0 a 5)',
                              'JamIndLevelOfTrafficSqrd': 'Nivel quadrático médio (0 a 25)',
                              'SctnQtdComprimento': "Comprimento do section (m)",
                              'JamQtdLengthMeters': "Comprimento médio de fila (m)",
                              'JamTimeDelayInSeconds': "Atraso médio (s)",
                              'SctnDscNome': "Rua",
                              'ClfuDscClassFunc': "Classificação Funcional"}, inplace=True)
    #Show
    cols = ["Rua",
            "Percentual de trânsito (min engarrafados / min monitorados)",
            "Comprimento médio de fila (m)",
            "Atraso médio (s)",
            "Nivel médio (0 a 5)",
            "Nivel quadrático médio (0 a 25)",
            "Velocidade média (km/h)",
            "Comprimento do section (m)",
            "SctnDscCoordxUtmComeco",
            "SctnDscCoordyUtmComeco",
            "Classificação Funcional"]
    pivot_jps = pivot_jps[cols]
    pivot_jps = pivot_jps.sort_values("Percentual de trânsito (min engarrafados / min monitorados)", ascending=False)
    
    return pivot_jps

def get_direction(coord_list):
    try:
      num_coords = len(coord_list)
    except:
      return pd.Series([None, None])
    
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

def adjust_for_dst()
    pass