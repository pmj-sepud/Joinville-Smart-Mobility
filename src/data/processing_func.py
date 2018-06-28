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

from sqlalchemy import MetaData, create_engine, extract, select, desc
from sqlalchemy.engine.url import URL

def connect_database(database_dict):

    DATABASE = database_dict

    db_url = URL(**DATABASE)
    engine = create_engine(db_url)
    meta = MetaData()
    meta.bind = engine

    return meta

def extract_df_jams(meta, date_begin, date_end, weekends=True, periods=None):
    meta.reflect(schema="waze")
    jams = meta.tables['waze.jams']
    data_files = meta.tables["waze.data_files"]

    query = select([data_files.c.start_time,
                    data_files.c.id,
                    jams.c.uuid,
                    jams.c.street,
                    jams.c.level,
                    jams.c.length,
                    jams.c.speed_kmh,
                    jams.c.delay,
                    jams.c.line])
    
    query = query.select_from(jams.join(data_files)).where(data_files.c.start_time.between(date_begin, date_end))

    if not weekends:
        query = query.where(extract("isodow", data_files.c.start_time).in_(list(range(1,6))))

    if periods:
        or_list=[]
        for t in periods:
            or_list.append(and_(extract("hour", data_files.c.start_time)>=t[0],
                                extract("hour", data_files.c.start_time)<t[1]
                                )
                          )
        query = query.where(or_(*or_list))
    
    if not weekends:
        query = query.where(extract("isodow", data_files.c.start_time).in_(list(range(1,6))))

    if periods:
        or_list=[]
        for t in periods:
            or_list.append(and_(extract("hour", data_files.c.start_time)>=t[0],
                                extract("hour", data_files.c.start_time)<t[1]
                                )
                          )
        query = query.where(or_(*or_list))
        
    query = query.order_by(desc(data_files.c.start_time))

    df_jams = pd.read_sql(query, meta.bind)
        
    return df_jams

def transform_geo_jams(df_jams):

    #Get Directions
    df_jams[["LonDirection","LatDirection", "MajorDirection"]] = df_jams["line"].apply(get_direction)

    #Get date information
    df_jams["start_time"] = pd.to_datetime(df_jams["start_time"], utc=True)
    df_jams["start_time"] = df_jams["start_time"].dt.tz_convert("America/Sao_Paulo")
    df_jams["date"] = df_jams["start_time"].dt.date
    df_jams["hour"] = df_jams["start_time"].astype(str).str[11:13].astype(int)
    df_jams["minute"] = df_jams["start_time"].astype(str).str[14:16].astype(int)
    df_jams["period"] = np.sign(df_jams["hour"]-12)

    #Get minute bins
    bins = [0, 14, 29, 44, 59]
    labels = []
    for i in range(1,len(bins)):
        if i==1:
            labels.append(str(bins[i-1]) + " a " + str(bins[i]))
        else:
            labels.append(str(bins[i-1]+1) + " a " + str(bins[i]))

    df_jams['minute_bin'] = pd.cut(df_jams["minute"], bins, labels=labels, include_lowest=True)

    #Get Geometries
    df_jams['jams_line_list'] = df_jams['line'].apply(lambda x: [tuple([d['x'], d['y']]) for d in x])
    #df_jams['jams_line_UTM'] = df_jams['jams_line_list'].apply(lon_lat_to_UTM)
    df_jams['linestring'] = df_jams.apply(lambda x: LineString(x['jams_line_list']), axis=1)
    crs_1 = {'init': 'epsg:4326'}
    geo_jams = gpd.GeoDataFrame(df_jams, crs=crs_1, geometry="linestring")
    crs_2 = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
    geo_jams = geo_jams.to_crs(crs_2)

    return geo_jams

def wkt_to_df(wkt_file):
    columns = {"objectid": "id_arcgis",
              "codlogra": "street_code",
              "nomelog": "street_name",
              "acumulo": "cumulative_meters",
              "st_length_": "length",
              "WKT": "wkt",
              }
    cols = list(columns.values())

    df_sections = (pd.read_csv(wkt_file, encoding="latin1", decimal=",")
                     .rename(columns=columns)
                     .reindex(columns=cols)
                     .dropna(subset=["street_name"])
                  )

    return df_sections

def transform_geo_sections(df_sections):
    """
    There has to be a column called "wkt" for this function to work.
    """

    def parse_wkt(df):
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

    df_sections = (df_sections
                      .pipe(parse_wkt)
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

    return geo_sections

def allocate_jams(jams, network, big_buffer, small_buffer, network_directional=False):

    """
    Common cross-referencing algorithm to be used by both ArcGis and OSM data.
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
    jams['jams_small_polygon'] = jams.apply(lambda x: x[jams_geometry_name].buffer(small_buffer), axis=1)
    jams['jams_big_polygon'] = jams.apply(lambda x: x[jams_geometry_name].buffer(big_buffer), axis=1)

    network['net_small_polygon'] = network.apply(lambda x: x[network_geometry_name].buffer(small_buffer), axis=1)
    network['net_big_polygon'] = network.apply(lambda x: x[network_geometry_name].buffer(big_buffer), axis=1)

    #Find jams that contain network arcs entirely
    jams = jams.set_geometry("jams_big_polygon") #big polygon will contain
    network = network.set_geometry("net_small_polygon") #small polygon will be contained
    merge_1 = gpd.sjoin(jams, network, how="left", op="contains", lsuffix='jams', rsuffix='net')
    list_unmatched_jams = merge_1[merge_1["index_net"].isnull()].index.tolist()
    merge_1.dropna(subset=["index_net"], inplace=True)
    merge_1.drop(labels="net_big_polygon", axis=1, inplace=True)

    #Find jams that are entirely contained by network arcs
    unallocated_jams = jams.loc[list_unmatched_jams].set_geometry("jams_small_polygon") #small polygon will be contained
    network = network.set_geometry("net_big_polygon") #big polygon will be contained

    merge_2 = gpd.sjoin(unallocated_jams, network, how="left", op="within", lsuffix='jams', rsuffix='net')
    list_unmatched_jams = merge_2[merge_2["index_net"].isnull()].index.tolist()
    merge_2.dropna(subset=["index_net"], inplace=True)
    merge_2.drop(labels="net_small_polygon", axis=1, inplace=True)

    #Find jams that intersect but with plausible directions (avoid perpendiculars).
    unallocated_jams = jams.loc[list_unmatched_jams].set_geometry("jams_small_polygon") #both polygons should be thin.
    network = network.set_geometry("net_small_polygon") #both polygons should be thin.

    merge_3 = gpd.sjoin(unallocated_jams, network, how="inner", op="intersects", lsuffix='jams', rsuffix='net')
    merge_3["match_directions"] = merge_3.direction_jams == merge_3.direction_net
    merge_3 = merge_3[merge_3["match_directions"]] #delete perpendicular streets
    merge_3.drop(labels=["match_directions", "net_big_polygon"] , axis=1, inplace=True)

    #Concatenate three dataframes
    allocated_jams = pd.concat([merge_1,
                                merge_2,
                                merge_3], ignore_index=True)
    
    allocated_jams.drop(labels=["jams_small_polygon", "jams_big_polygon"] , axis=1, inplace=True)
    
    return allocated_jams

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
        lat_direction = "North"
    else:
        lat_direction = "South"
        
    #East/West
    x_start = coord_list[0]["x"]
    x_end = coord_list[num_coords-1]["x"]
    delta_x = (x_end-x_start)
    if delta_x >= 0:
        lon_direction = "East"
    else:
        lon_direction = "West"

    #MajorDirection
    if abs(delta_y) > abs(delta_x):
        major_direction = "North/South"
    else:
        major_direction = "East/West"

        
    return pd.Series([lon_direction, lat_direction, major_direction])
