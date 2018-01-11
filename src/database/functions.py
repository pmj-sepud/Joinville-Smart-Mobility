import pandas as pd
from sqlalchemy.types import JSON as typeJSON
from src.common import exceptions
from src.data.functions import UTM_to_lon_lat
from shapely.geometry import LineString


def prep_rawdata_tosql(raw_data):

    raw_data_tosql = raw_data[["startTime", "endTime"]]
    rename_dict = {"startTime": "MgrcDateStart",
                   "endTime": "MgrcDateEnd",
                  }

    raw_data_tosql = raw_data_tosql.rename(columns=rename_dict)

    return raw_data_tosql

def build_df_trechos(meta):
    trecho = meta.tables['Trecho']
    trechos_query = trecho.select()
    df_trechos = pd.read_sql(trechos_query, con=meta.bind, index_col="TrchId")

    #Create Geometry shapes
    df_trechos["Street_line_XY"] = df_trechos.apply(lambda x: [tuple([x['TrchDscCoordxUtmComeco'], x['TrchDscCoordyUtmComeco']]),
                                                               tuple([x['TrchDscCoordxUtmMeio'], x['TrchDscCoordyUtmMeio']]),
                                                               tuple([x['TrchDscCoordxUtmFinal'], x['TrchDscCoordyUtmFinal']]),
                                                              ], axis=1)

    df_trechos["Street_line_LonLat"] = df_trechos['Street_line_XY'].apply(UTM_to_lon_lat)
    df_trechos['trecho_LineString'] = df_trechos.apply(lambda x: LineString(x['Street_line_XY']).buffer(12), axis=1)
        
    return df_trechos

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

def prep_jpt_tosql(jams_per_trecho):

    jpt_tosql = jams_per_trecho[["impacted_trechos", "jams_uuid", "startTime" ]]
    rename_dict = {"impacted_trechos": "TrchId",
                   "startTime": "JamDateStart",
                   "jams_uuid": "JamUuid",
                  }

    jpt_tosql = jpt_tosql.rename(columns=rename_dict)

    return jpt_tosql