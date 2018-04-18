import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir)
sys.path.append(project_dir)

import unittest
import dotenv
import pandas as pd
import json
from io import StringIO
import pytz
from sqlalchemy import create_engine, exc, MetaData
from sqlalchemy.engine.url import URL
from sqlalchemy.types import JSON as typeJSON
import datetime
import math
from bson.objectid import ObjectId
from pymongo import MongoClient
from shapely.geometry import Point

from src.data.processing_func import (connect_database, collect_records, tabulate_records, json_to_df,
                                tabulate_jams, lon_lat_to_UTM, UTM_to_lon_lat,
                                prep_jams_tosql, prep_rawdata_tosql, extract_geo_sections,
                                prep_section_tosql, store_jps)

from src.data.load_func import (extract_jps)

dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

class TestProcessingFunc(unittest.TestCase):
    
    def test_collect_records(self):
        uri = os.environ.get("mongo_uri")
        client = MongoClient(uri)
        db = client.ccp
        collection = db.ccp_collection
        
        records = collect_records(collection, limit=1)
       
        self.assertEqual(type(records), list)
        self.assertEqual(len(records), 1)

        client.close()
        
        
    def test_tabulate_records(self):
        """
        1 - Date is in correct timezone
        2 - 
        """

        doc = open(project_dir + "/tests/test_data/test_records.txt", "r")
        json_string = json.load(doc)
        json_io = StringIO(json_string)
        records = json.load(json_io)
        raw_data = tabulate_records(records)

        self.assertEqual(type(raw_data), pd.DataFrame)
        self.assertEqual(type(raw_data['startTime'][0]), pd.Timestamp)
        self.assertEqual(raw_data['startTime'][0].tz.zone, pytz.timezone("America/Sao_Paulo").zone)
        self.assertEqual(raw_data['startTimeMillis'].dtype, int)
        self.assertEqual(raw_data['endTimeMillis'].dtype, int)
        if 'jams' in raw_data:
            self.assertEqual(raw_data['jams'].dtype, dict)
        if 'alerts' in raw_data:
            self.assertEqual(raw_data['alerts'].dtype, dict)
        if 'irregularities' in raw_data:
            self.assertEqual(raw_data['irregularities'].dtype, dict)

        doc.close()
           
    def test_tabulate_jams(self):
        sample_json = { "_id" : ObjectId("59cc0811d34a9512bab73343"),
                        "startTime" : "2017-09-27 20:17:00:000",
                        "endTimeMillis" : 1506543480000,
                        "startTimeMillis" : 1506543420000,
                        "endTime" : "2017-09-27 20:18:00:000",
                        "jams" : [{ "turnType" : "NONE",
                                    "delay" : 82,
                                    "roadType" : 1,
                                    "street" : "R. Alm. Jaceguay",
                                    "uuid" : 1174570,
                                    "line" : [{"y" : -26.273961,"x" : -48.879597},
                                              {"x" : -48.878684, "y" : -26.273931}],
                                    "pubMillis" : 1506541721537,
                                    "country" : "BR",
                                    "speed" : 4.55277777777778,
                                    "length" : 743,
                                    "segments" : [{},],
                                    "type" : "NONE",
                                    "city" : "Joinville"
                                  },
                                  {"turnType" : "NONE",
                                   "level" : 2,
                                   "delay" : 96,
                                   "roadType" : 2,
                                   "street" : "R. Timbó",
                                   "endNode" : "R. Dr. João Colin",
                                   "uuid" : 3246489,
                                   "line" : [{"y" : -26.293511,"x" : -48.852581},
                                             {"y" : -26.293693, "x" : -48.850126},
                                            ],
                                   "pubMillis" : 1506542575993,
                                   "country" : "BR",
                                   "speed" : 2.96388888888889,
                                   "length" : 454,
                                   "segments" : [{},{}],
                                   "type" : "NONE",
                                   "city" : "Joinville"
                                  },
                                 ],
                      }
        sample_df = pd.DataFrame(sample_json)
        test_df = tabulate_jams(sample_df)

        
        self.assertEqual(test_df.shape, (2, 21))
        self.assertEqual(test_df['_id'].iloc[0], test_df['_id'].iloc[0])
        self.assertTrue(pd.isnull(test_df['jams_level'].iloc[0]))

    def test_json_to_df(self):
        test_jam = [{'uid':'Jam 1',
                     'coluna1.1': 'conteúdo A',
                     'coluna1.2': 'conteúdo B',
                     'coluna1.3': 'conteúdo C',
                     'coluna1.4':[{'1.4.1': 'conteúdo D',
                             '1.4.2': 'conteúdo E',
                             '1.4.3': 'conteúdo F',
                            }],
                    },
                    {'uid': 'Jam 2',
                     'coluna1.1': 'conteúdo G'},
                    {'uid': 'Jam 3',
                     'coluna1.1': 'conteúdo H',
                     'coluna1.2': 'conteúdo I',
                     'coluna1.3': 'conteúdo J',
                    }
                   ]
                      
        data = {'coluna A': 'a',
                'coluna B': 'b',
                'coluna C': test_jam,
               }
                     
        test_row = pd.Series(data)

        df = json_to_df(test_row, 'coluna C')
        
        self.assertEqual(type(df), pd.DataFrame)
        self.assertEqual(df.shape, (3, 8))
        self.assertEqual(df['coluna A'][0], 'a')
        self.assertEqual(df['coluna B'][0], 'b')
        self.assertEqual(df[df['coluna C_uid'] == 'Jam 1']['coluna C_coluna1.1'].iloc[0], 'conteúdo A')
        self.assertEqual(type(df[df['coluna C_uid'] == 'Jam 1']['coluna C_coluna1.4'].iloc[0]), list)
        self.assertTrue(pd.isnull(df[df['coluna C_uid'] == 'Jam 3']['coluna C_coluna1.4'].iloc[0]))
        self.assertEqual(df['coluna A'].iloc[0], df['coluna A'].iloc[1])
    
    def test_lon_lat_to_UTM(self):
        l = [(-48.85777, -26.31254), (-48.84572, -26.30740)]
        UTM_list = lon_lat_to_UTM(l)
        
        self.assertTrue(math.isclose(UTM_list[0][1], 7087931, rel_tol=1e-7))
        self.assertTrue(math.isclose(UTM_list[1][0], 715062, rel_tol=1e-6))

    def test_prep_jams_tosql(self):

        test_df_jams = pd.read_csv(project_dir + "/tests/test_data/test_df_jams.csv")
        test_jams_tosql = prep_jams_tosql(test_df_jams)
        
        self.assertEqual(len(test_jams_tosql.columns),16)
        self.assertEqual(test_jams_tosql["JamTimeDelayInSeconds"].dtype, int)
        self.assertEqual(test_jams_tosql["JamDateEnd"].dtype, datetime.datetime)
        self.assertEqual(type(test_jams_tosql["JamDscStreet"].iloc[0]), str)
        self.assertEqual(type(test_jams_tosql["JamDscCoordinatesLonLat"].iloc[0]), str)
        
    def test_UTM_to_lon_lat(self):
        l = [(713849, 7087931), (715062, 7088480)]
        lon_lat_list = UTM_to_lon_lat(l)

        self.assertTrue(math.isclose(lon_lat_list[0][0], -48.85777, rel_tol=1e-5))
        self.assertTrue(math.isclose(lon_lat_list[1][1], -26.30740, rel_tol=1e-5))

    def test_extract_geo_sections(self):
        #Connection and initial setup
        DATABASE = {
        'drivername': os.environ.get("db_drivername"),
        'host': os.environ.get("db_host"), 
        'port': os.environ.get("db_port"),
        'username': os.environ.get("db_username"),
        'password': os.environ.get("db_password"),
        'database': os.environ.get("db_database"),
        }
        meta = connect_database(DATABASE)

        test_geo_sections = extract_geo_sections(meta)

        self.assertEqual(test_geo_sections.shape, (16148, 16))
        self.assertEqual(test_geo_sections.geometry.name, "section_polygon")

class TestLoadFunc(unittest.TestCase):
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
    meta = MetaData()
    meta.bind = engine
    meta.reflect()

    def test_extract_jps_datefilter(self):
        """
        Date filter works properly
        """
        date_begin = datetime.date(day=27, month=9, year=2017)
        date_end = datetime.date(day=29, month=9, year=2017)
        df_jps = extract_jps(self.meta, date_begin, date_end)

        self.assertEqual(df_jps["MgrcDateStart"].dt.date.nunique(), 2)

    def test_extract_jps_timefilter(self):
        """
        2 - Time filter works properly
        """
        date_begin = datetime.date(day=28, month=9, year=2017)
        date_end = datetime.date(day=29, month=9, year=2017)
        periods = [(7,9), (17,19)]
        df_jps = extract_jps(self.meta, date_begin, date_end, periods=periods)

        self.assertEqual(df_jps["MgrcDateStart"].dt.hour.nunique(), 4)

    def test_extract_jps_weekendsfilter(self):
        """
        3 - Weekends filter works properly
        """
        date_begin = datetime.date(day=28, month=9, year=2017)
        date_end = datetime.date(day=10, month=10, year=2017)
        periods = [(17,19)]
        df_jps_wkTrue = extract_jps(self.meta, date_begin, date_end, periods=periods, weekends=True)
        df_jps_wkFalse = extract_jps(self.meta, date_begin, date_end, periods=periods, weekends=False)

        self.assertEqual(df_jps_wkTrue["MgrcDateStart"].dt.dayofweek.nunique(), 7)
        self.assertEqual(df_jps_wkFalse["MgrcDateStart"].dt.dayofweek.nunique(), 5) 

    def test_extract_jps_binsdivision(self):
        """
        Bins are set properly
        """
        date_begin = datetime.date(day=27, month=9, year=2017)
        date_end = datetime.date(day=28, month=9, year=2017)
        df_jps = extract_jps(self.meta, date_begin, date_end)
        df_jps['minute_bin_check'] = (df_jps["MgrcDateStart"].dt.minute < df_jps["minute_bin"].str[0:2].astype(int)) \
                                     | (df_jps["MgrcDateStart"].dt.minute > df_jps["minute_bin"].str[-2:].astype(int))

        self.assertEqual(df_jps["minute_bin_check"].sum(), 0)


    