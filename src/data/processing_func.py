import os
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from pyproj import Proj
import geojson
from pymongo import MongoClient, DESCENDING
from shapely.geometry import LineString, MultiLineString
from shapely.wkt import loads as wkt_loads
import geopandas as gpd
import math
from timeit import default_timer as timer

from sqlalchemy import MetaData, create_engine, extract, select
from sqlalchemy.engine.url import URL

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

    raw_data['startTime'] = raw_data['startTime'].dt.tz_localize("UTC")
    raw_data['endTime'] = raw_data['endTime'].dt.tz_localize("UTC")

    raw_data['startTime'] = raw_data['startTime'].dt.tz_convert("America/Sao_Paulo")
    raw_data['endTime'] = raw_data['endTime'].dt.tz_convert("America/Sao_Paulo")

    raw_data['startTime'] = raw_data['startTime'].astype(pd.Timestamp)
    raw_data['endTime'] = raw_data['endTime'].astype(pd.Timestamp)

    return raw_data

def connect_database(database_dict):

    DATABASE = database_dict

    db_url = URL(**DATABASE)
    engine = create_engine(db_url)
    meta = MetaData()
    meta.bind = engine

    return meta

def prep_section_tosql(section_path):
    columns = {"objectid": "id_arcgis",
              "codlogra": "street_code",
              "nomelog": "street_name",
              "acumulo": "cumulative_meters",
              "st_length_": "length",
              "WKT": "wkt",
              }
    cols = list(columns.values())

    df_sections = (pd.read_csv(section_path, encoding="latin1", decimal=",")
                     .rename(columns=columns)
                     .reindex(columns=cols)
                     .dropna(subset=["street_name"])
                  )

    return df_sections


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

# This code does not longer applies. Now there has to be a common cross-referencing algorithm to be used
# by both GEO and OSM data.

def allocate_jams(jams, network, big_buffer, small_buffer, network_directional=False):

    """
    The geometry must be a Linestring for both GeoDataFrames
    """

    def get_main_direction(geometry):
        if type(geometry) is LineString:
            delta_x = geometry.coords[-1][0] - geometry.coords[0][0]
            delta_y = geometry.coords[-1][1] - geometry.coords[0][1]
        elif type(geometry) is MultiLineString:
            first_line = list(geometry.geoms)[0]
            last_line = list(geometry.geoms)[-1]

            delta_x = last_line.coords[-1][0] - first_line.coords[0][0]
            delta_y = last_line.coords[-1][1] - first_line.coords[0][1]
        else:
            raise Exception("geometry must be a Linestring or MultiLineString")

        if network_directional == True:
          if abs(delta_y) >= abs(delta_x):
              if delta_y >=0:
                  return "Norte"
              else:
                  return "Sul"
          else:
              if delta_x >=0:
                  return "Leste"
              else:
                  return "Oeste"

        if network_directional == False:
          if abs(delta_y) >= abs(delta_x):
              return "Norte/Sul"
          else:
              return "Leste/Oeste"

    def check_directions(x):
        """
        Check for jams whose direction is not aligned with the direction of the street or the section.
        Ex.: perpendicular streets, which would intersect with the jam.
        """

        if x["direction_left"] == x["direction_right"]:
            return True
        else:
            return False

    #check CRS
    crs = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    if jams.crs != crs:
        jams = jams.to_crs(crs)
        jams.crs = crs

    if network.crs != crs:
        network = network.to_crs(crs)
        network.crs = crs

    #Get linestring directions for both jams and network
    jams_geometry_name = jams.geometry.name
    network_geometry_name = network.geometry.name 

    jams = (jams
            .assign(direction=lambda gdf: pd.Series([get_main_direction(row[jams_geometry_name]) for _, row in gdf.iterrows()],
                                                    index=gdf.index),                      
            )
    )

    network = (network
            .assign(direction=lambda gdf: pd.Series([get_main_direction(row[network_geometry_name]) for _, row in gdf.iterrows()],
                                                    index=gdf.index),                      
            )
    )

    #Create big and small polygons
    jams['small_polygon'] = jams.apply(lambda x: x[jams_geometry_name].buffer(small_buffer), axis=1)
    jams['big_polygon'] = jams.apply(lambda x: x[jams_geometry_name].buffer(big_buffer), axis=1)

    network['small_polygon'] = network.apply(lambda x: x[network_geometry_name].buffer(small_buffer), axis=1)
    network['big_polygon'] = network.apply(lambda x: x[network_geometry_name].buffer(big_buffer), axis=1)

    #Find jams that contain network arcs entirely
    jams = jams.set_geometry("big_polygon") #big polygon will contain
    network = network.set_geometry("small_polygon") #small polygon will be contained
    merge_1 = gpd.sjoin(jams, network, how="left", op="contains")
    list_unmatched_jams = merge_1[merge_1["index_right"].isnull()].index.tolist()
    merge_1.dropna(subset=["index_right"], inplace=True)

    #Find jams that are entirely contained by network arcs
    unallocated_jams = jams.loc[list_unmatched_jams].set_geometry("small_polygon") #small polygon will be contained
    network = network.set_geometry("big_polygon") #big polygon will be contained

    merge_2 = gpd.sjoin(unallocated_jams, network, how="left", op="within")
    list_unmatched_jams = merge_2[merge_2["index_right"].isnull()].index.tolist()
    merge_2.dropna(subset=["index_right"], inplace=True)

    #Find jams that intersect but with plausible directions (avoid perpendiculars).
    unallocated_jams = jams.loc[list_unmatched_jams].set_geometry("small_polygon") #both polygons should be thin.
    network = network.set_geometry("small_polygon") #both polygons should be thin.

    merge_3 = gpd.sjoin(unallocated_jams, network, how="inner", op="intersects")
    merge_3["match_directions"] = merge_3.direction_left == merge_3.direction_right
    merge_3 = merge_3[merge_3["match_directions"]] #delete perpendicular streets
    merge_3.drop(labels="match_directions", axis=1, inplace=True)

    #Concatenate three dataframes
    allocated_jams = pd.concat([merge_1,
                                merge_2,
                                merge_3], ignore_index=True)


    return allocated_jams

"""
def store_jps(meta, batch_size=20000):
    def check_directions(x):
        Check for jams whose direction is not aligned with the direction of the street or the section.
        Ex.: perpendicular streets, which would intersect with the jam.

        if x["MajorDirection"] == x["StreetDirection"]:
            return True
        elif x["MajorDirection"] == x["SectionDirection"]:
            return True
        else:
            return False

    geo_sections = extract_geo_sections(meta, main_buffer=10, alt_buffer=20) #thin polygon

    ##Divide the in batches
    total_rows, = meta.tables["Jam"].count().execute().first()
    number_batches = math.ceil(total_rows / batch_size)

    for i in range(0, number_batches):
        start = timer()
        geo_jams = extract_geo_jams(meta, skip=i*batch_size, limit=batch_size, main_buffer=20, alt_buffer=10) #fat polygon

        #Find jams that contain sections entirely
        jams_per_section_contains = gpd.sjoin(geo_jams, geo_sections, how="left", op="contains")
        ids_not_located_contains = jams_per_section_contains[jams_per_section_contains["SctnId"].isnull()]["JamId"]
        jams_per_section_contains.dropna(subset=["SctnId"], inplace=True)

        #Find jams that are entirely within sections
        jams_left_from_contains = geo_jams.loc[geo_jams["JamId"].\
                                  isin(ids_not_located_contains)].\
                                  set_geometry("jam_alt_LineString") #thin jam polygon

        geo_sections = geo_sections.set_geometry("section_alt_LineString") #fat section polygon
        jams_per_section_within = gpd.sjoin(jams_left_from_contains, geo_sections, how="left", op="within")
        ids_not_located_within = jams_per_section_within[jams_per_section_within["SctnId"].isnull()]["JamId"]
        jams_per_section_within.dropna(subset=["SctnId"], inplace=True)

        #Find jams that intersect but with plausible directions (avoid perpendiculars).
        geo_sections = geo_sections.set_geometry("section_LineString") #Both polygons should be thin.
        jams_left_from_within = geo_jams.loc[geo_jams["JamId"].isin(ids_not_located_within)]
        jams_per_section_intersects = gpd.sjoin(jams_left_from_within, geo_sections, how="inner", op="intersects")
        jams_per_section_intersects["CheckDirections"] = jams_per_section_intersects.apply(lambda x: check_directions(x), axis=1)
        jams_per_section_intersects = jams_per_section_intersects[jams_per_section_intersects["CheckDirections"]] #delete perpendicular streets
        jams_per_section_intersects.drop(labels="CheckDirections", axis=1, inplace=True)

        #Concatenate three dataframes
        jams_per_section = pd.concat([jams_per_section_contains,
                                      jams_per_section_within,
                                      jams_per_section_intersects], ignore_index=True)

        #Store in database
        jams_per_section = jams_per_section[["JamDateStart", "JamUuid", "SctnId"]]  
        jams_per_section["JamDateStart"] = jams_per_section["JamDateStart"].astype(pd.Timestamp)
        jams_per_section.to_sql("JamPerSection", con=meta.bind, if_exists="append", index=False)
        end = timer()
        duration = str(round(end - start))
        print("Batch " + str(i+1) + " of " + str(number_batches) + " took " + duration + " s to be successfully stored.")
"""

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

def extract_geo_sections(meta):

    def read_wkt(df):
        df["linestring"] = df.wkt.apply(lambda x: wkt_loads(x))
        df = df.drop("wkt", axis=1)
        return df

    def get_main_direction(min_x, min_y, max_x, max_y):
        delta_x = max_x - min_x
        delta_y = max_y - min_y
        if delta_y >= delta_x:
            return "Norte/Sul"
        else:
            return "Leste/Oeste"

    meta.reflect(schema="geo")
    section = meta.tables['geo.sections']
    sections_query = section.select()
    df_sections = pd.read_sql(sections_query, con=meta.bind, index_col="id")
    df_sections = (df_sections
                      .pipe(read_wkt)
                      .assign(min_x=lambda df: pd.Series([item.bounds[0] for _, item in df.linestring.iteritems()],
                                                         index=df.index),
                              min_y=lambda df: pd.Series([item.bounds[1] for _, item in df.linestring.iteritems()],
                                                         index=df.index),
                              max_x=lambda df: pd.Series([item.bounds[2] for _, item in df.linestring.iteritems()],
                                                         index=df.index),
                              max_y=lambda df: pd.Series([item.bounds[3] for _, item in df.linestring.iteritems()],
                                                         index=df.index),                           
                             )
                  )

    #Get Street Direction
    gb = (df_sections.groupby("street_name")
                     .agg({"min_x": "min",
                          "min_y": "min",
                          "max_x": "max",
                          "max_y": "max",})
                     .assign(street_direction=lambda df: pd.Series([get_main_direction(min_x=row.min_x,
                                                                                       min_y=row.min_y,
                                                                                       max_x=row.max_x,
                                                                                       max_y=row.max_y) for row in df.itertuples(index=False)],
                                                                   index=df.index)
                            )
                     .loc[:, "street_direction"]
         )

    df_sections = df_sections.join(gb, on="street_name")

    df_sections = (df_sections
                  .assign(section_direction=lambda df: pd.Series([get_main_direction(min_x=row.min_x,
                                                                                     min_y=row.min_y,
                                                                                     max_x=row.max_x,
                                                                                     max_y=row.max_y) for row in df.itertuples(index=False)],
                                                                 index=df.index),                      
                         )
                  )

    crs = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    geo_sections = gpd.GeoDataFrame(df_sections, crs=crs, geometry="linestring")
    #geo_sections = geo_sections.to_crs({'init': 'epsg:4326'})

    return geo_sections

def extract_geo_jams(meta, skip=0, limit=None):
    meta.reflect(schema="waze")
    jams = meta.tables['waze.jams']
    jams_query = select([jams.c.uuid,
                          jams.c.pub_utc_date,
                          jams.c.street,
                          jams.c.delay,
                          jams.c.speed,
                          jams.c.speed_kmh,
                          jams.c.length,
                          jams.c.level,
                          jams.c.line,
                          jams.c.datafile_id,
                             ]).order_by(jams.c.pub_utc_date).offset(skip).limit(limit)
    df_jams = pd.read_sql(jams_query, con=meta.bind)
    df_jams['jams_line_list'] = df_jams['line'].apply(lambda x: [tuple([d['x'], d['y']]) for d in x])
    df_jams['jams_line_UTM'] = df_jams['jams_line_list'].apply(lon_lat_to_UTM)
    df_jams['linestring'] = df_jams.apply(lambda x: LineString(x['jams_line_UTM']), axis=1)
    
    df_jams[["LonDirection","LatDirection", "MajorDirection"]] = df_jams["line"].apply(get_direction)

    crs = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    geo_jams = gpd.GeoDataFrame(df_jams, crs=crs, geometry="linestring")
    #geo_jams = geo_jams.to_crs({'init': 'epsg:4326'})

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

def get_direction(coord_list):
    try:
      num_coords = len(coord_list)
    except:
      return pd.Series([None, None])
    
    #North/South
    y_start = coord_list[0]["y"]
    y_end = coord_list[num_coords-1]["y"]
    delta_y = (y_end-y_start)
    if delta_y >= 0:
        lat_direction = "Norte"
    else:
        lat_direction = "Sul"
        
    #East/West
    x_start = coord_list[0]["x"]
    x_end = coord_list[num_coords-1]["x"]
    delta_x = (x_end-x_start)
    if delta_x >= 0:
        lon_direction = "Leste"
    else:
        lon_direction = "Oeste"

    #MajorDirection
    if abs(delta_y) > abs(delta_x):
        major_direction = "Norte/Sul"
    else:
        major_direction = "Leste/Oeste"

        
    return pd.Series([lon_direction, lat_direction, major_direction])
