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
from datetime import datetime, date
import math
from bson.objectid import ObjectId
from pymongo import MongoClient
from shapely.geometry import Point

from src.data.processing_func import (collect_records, tabulate_records, json_to_df,
                                tabulate_jams, lon_lat_to_UTM, UTM_to_lon_lat,
                                prep_jams_tosql, extract_geo_sections)

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
        pass

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

    def test_prep_jams_tosql(self):

        test_df_jams = pd.read_csv(project_dir + "/tests/test_data/test_df_jams.csv")
        test_jams_tosql = prep_jams_tosql(test_df_jams)
        
        self.assertEqual(len(test_jams_tosql.columns),16)
        self.assertEqual(test_jams_tosql["JamTimeDelayInSeconds"].dtype, int)
        self.assertEqual(test_jams_tosql["JamDateEnd"].dtype, datetime)
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

        db_url = URL(**DATABASE)
        engine = create_engine(db_url)
        meta = MetaData()
        meta.bind = engine
        meta.reflect()

        test_geo_sections = extract_geo_sections(meta)
        
        self.assertEqual(test_geo_sections.shape, (16248, 15))
        self.assertTrue(len(test_geo_sections['Street_line_XY'].iloc[0]), 3)
        self.assertTrue(len(test_geo_sections['Street_line_LonLat'].iloc[0]), 3)
        self.assertTrue(test_geo_sections['section_LineString'].iloc[0].distance(Point(test_geo_sections['Street_line_LonLat'].iloc[0][0])) < 1e-1)
        self.assertTrue(math.isclose(test_geo_sections[['SctnDscCoordxUtmComeco', 'SctnDscCoordxUtmMeio', 'SctnDscCoordxUtmFinal']].values.min(), 699657, rel_tol=1e-6))
        self.assertTrue(math.isclose(test_geo_sections[['SctnDscCoordxUtmComeco', 'SctnDscCoordxUtmMeio', 'SctnDscCoordxUtmFinal']].values.max(), 723463, rel_tol=1e-6))    
        self.assertTrue(math.isclose(test_geo_sections[['SctnDscCoordyUtmComeco', 'SctnDscCoordyUtmMeio', 'SctnDscCoordyUtmFinal']].values.min(), 7078083, rel_tol=1e-7))
        self.assertTrue(math.isclose(test_geo_sections[['SctnDscCoordyUtmComeco', 'SctnDscCoordyUtmMeio', 'SctnDscCoordyUtmFinal']].values.max(), 7108810, rel_tol=1e-7))


class TestLoadFunc(unittest.TestCase):

    def test_extract_jps(self):
        pass

    def test_transf_flow_features(self):
        pass

    def test_transf_flow_labels(self):
        pass

"""
    def test_normalize_jps(self):

        jps_to_normalize = pd.read_csv("../test_data/test_jps_to_normalize.csv", index_col=0)
        jps_to_normalize["JamDateStart"] = jps_to_normalize["JamDateStart"].astype('datetime64[ns]')

        norm_df_jps = normalize_jps(jps_to_normalize)

        #import pdb
        #pdb.set_trace()

        test_date = date(year=2017, month=11, day=25)

        self.assertEqual(norm_df_jps.shape, (2,10))
        self.assertEqual(norm_df_jps[norm_df_jps["JamDateStart"] < test_date]["JamIndLevelOfTraffic"].iloc[0], 2)


    def test_get_pivot_jps_means(self):
        norm_df_jps = pd.read_csv("../test_data/test_norm_df_jps.csv")

        pivot_jps_means = get_pivot_jps_means(norm_df_jps)

        self.assertEqual(pivot_jps_means.shape, (1,6))
        self.assertEqual(pivot_jps_means["JamIndLevelOfTraffic"].iloc[0], 3)


    def test_get_pivot_jps_count(self):
        norm_df_jps = pd.read_csv("../test_data/test_norm_df_jps.csv")

        pivot_jps_count = get_pivot_jps_count(norm_df_jps)

        self.assertEqual(pivot_jps_count.shape, (1,1))
        self.assertEqual(pivot_jps_count["JamDateStart"].iloc[0], 2)

    def test_gen_pivot_table(self):

        df_jps = pd.read_csv("../test_data/test_jps_to_normalize.csv")

        total_observations = df_jps['JamDateStart'].nunique()

        pivot_table = gen_pivot_table(df_jps, total_observations)

        self.assertEqual(pivot_table.shape, (1,9))
        self.assertEqual(pivot_table.iloc[0]["Percentual de trânsito (min engarrafados / min monitorados)"],1)
"""



    