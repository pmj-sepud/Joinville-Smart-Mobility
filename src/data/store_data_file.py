import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import json
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
import hashlib 
import time
import boto3

from sqlalchemy import create_engine, exc, MetaData, select
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import or_, and_
from sqlalchemy.types import JSON as typeJSON

def connect_database(database_dict):

    DATABASE = database_dict

    db_url = URL(**DATABASE)
    engine = create_engine(db_url)
    meta = MetaData()
    meta.bind = engine

    return meta

def tab_raw_data(s3key, s3object):

    def build_raw_df(rec_list):
        df_list = []
        for rec in rec_list:
            rec["rec_string"] = json.dumps(rec, sort_keys=True)
            rec = json_normalize(rec)
            df_list.append(rec)
        raw_data = pd.concat(df_list)
        return raw_data

    #read data file and convert it to a list of dicts.
    file = s3object['Body'].read()
    rec_list = json.loads(file)
    if type(rec_list) is dict:
        rec_list = [rec_list]
    if type(rec_list) is not list:
        raise Exception("It should be a list.")

    raw_data = build_raw_df(rec_list)

    #Get rid of oid (pymongo) if it exists. From now on, object is the same regardless of source.
    raw_data.drop("_id.$oid", axis=1, inplace=True, errors='ignore')

    #clean date fields
    raw_data['startTime'] = pd.to_datetime(raw_data['startTime'].str[:-4])
    raw_data['endTime'] = pd.to_datetime(raw_data['endTime'].str[:-4])
    raw_data['startTime'] = raw_data['startTime'].astype(pd.Timestamp)
    raw_data['endTime'] = raw_data['endTime'].astype(pd.Timestamp)

    #get creation time
    raw_data['date_created'] = pd.to_datetime('today')

    #during creation time, date_updated equals date_created
    raw_data['date_updated'] = raw_data['date_created']

    #get_file_name
    raw_data['file_name'] = s3key

    #get DataFile hash
    raw_data['json_hash'] = raw_data["rec_string"].apply(lambda x: hashlib.sha1(x.encode()).hexdigest()) 

    return raw_data

def sep_aji_records(raw_data, aji_type):
    recs = []
    for aji in raw_data[aji_type].iloc[0]:
        aji["rootStartTime"] = raw_data.startTimeMillis.iloc[0]
        df_aji = json_normalize(aji)
        df_aji["rec"] = json.dumps(aji, sort_keys=True)
        recs.append(df_aji)
    df = pd.concat(recs)
    return df

def hash_raw_aji(raw_aji):
    aji_hash = hashlib.sha1(raw_aji.encode()).hexdigest()
    return aji_hash

def align_all_columns(df, col_list):
    all_columns = pd.DataFrame(np.nan, index=[0], columns=col_list)
    df,_ = df.align(all_columns, axis=1)
    return df

def tab_jams(raw_data):
    if ('jams' not in raw_data) or (raw_data.jams.iloc[0] is np.nan):
        print("No jams in this data file.")
        return

    col_dict = {
                'blockingAlertUuid': "blocking_alert_id",
                'startNode': "start_node",
                 'endNode': "end_node",
                 'pubMillis': "pub_millis",
                 'roadType': "road_type",
                 'speedKMH': "speed_kmh",
                 'turnType': "turn_type",
                 }

    other_cols = ['id','city', 'country','delay', 'length',
                  'uuid', 'street', 'level', 'line', 'pub_utc_date']
    col_list = list(col_dict.values())
    col_list = col_list + other_cols

    df_jams = sep_aji_records(raw_data, "jams")
    df_jams = (df_jams
               .rename(columns=col_dict)
               .pipe(align_all_columns, col_list=col_list)
               .assign(pub_utc_date=lambda x: pd.to_datetime(x["pub_millis"], unit='ms'),
                       id=df_jams["rec"].apply(hash_raw_aji)
                      )
              )
    df_jams = df_jams[col_list]

    return df_jams

def tab_irregularities(raw_data):
    if ('irregularities' not in raw_data) or (raw_data.irregularities.iloc[0] is np.nan):
        print("No irregularities in this data file.")
        return

    col_dict = {
                'detectionDateMillis': "detection_date_millis",
                'detectionDate': "detection_date",
                'updateDateMillis': "update_date_millis",
                'updateDate': "update_date",
                'regularSpeed': "regular_speed",
                'delaySeconds': "delay_seconds",
                'jamLevel': "jam_level",
                'driversCount': "drivers_count",
                'alertsCount': "alerts_count",
                'nThumbsUp': "n_thumbs_up",
                'nComments': "n_comments",
                'nImages': "n_images",
                'id': "uuid",
                 }

    other_cols = ['id', 'street', 'city', 'country', 'speed',
                  'seconds', 'length', 'trend', 'type', 'severity', 'line']
    col_list = list(col_dict.values())
    col_list = col_list + other_cols

    df_irregs = sep_aji_records(raw_data, "irregularities")
    df_irregs = (df_irregs
                 .rename(columns=col_dict)
                 .pipe(align_all_columns, col_list=col_list)
                 .assign(detection_utc_date=lambda x: pd.to_datetime(x["detection_date_millis"], unit='ms'),
                         update_utc_date=lambda x: pd.to_datetime(x["update_date_millis"], unit='ms'),
                         id=df_irregs["rec"].apply(hash_raw_aji)
                        )
              )
    df_irregs = df_irregs[col_list]

    return df_irregs

def tab_alerts(raw_data):
    if ('alerts' not in raw_data) or (raw_data.alerts.iloc[0] is np.nan):
        print("No alerts in this data file.")
        return
   
    col_dict = {
                'pubMillis': "pub_millis",
                'roadType': "road_type",
                'reportDescription': "report_description",
                'reportRating': "report_rating",
                'nThumbsUp': "thumbs_up",
                'jamUuid': "jam_uuid",
                'reportByMunicipalityUser': 'report_by_municipality_user',
                 }

    other_cols = ['id', 'uuid', 'street', 'city', 'country', 'location', 'magvar',
                  'reliability', 'type', 'subtype' ]
    col_list = list(col_dict.values())
    col_list = col_list + other_cols

    df_alerts = sep_aji_records(raw_data, "alerts")
    df_alerts = (df_alerts
                 .rename(columns=col_dict)
                 .pipe(align_all_columns, col_list=col_list)
                 .assign(location=df_alerts.apply(lambda row: {'x': row["location.x"], 'y': row["location.y"]},
                                                  axis=1),
                         pub_utc_date=lambda x: pd.to_datetime(x["pub_millis"], unit='ms'),
                         id=df_alerts["rec"].apply(hash_raw_aji)
                        )

                )
    df_alerts = df_alerts[col_list]

    return df_alerts

#Connection and initial setup
DATABASE = {
'drivername': "postgresql",
'host': "localhost", 
'port': 5432,
'username': "tester",
'password': "testmobility123",
'database': "test_mobility",
}

meta = connect_database(DATABASE)

s3 = boto3.client('s3')
bucket="scripted-waze-data-929310922828-test"
#Iterate over all s3 raw data objects
all_data_files = []
paginator = s3.get_paginator('list_objects')
page_iterator = paginator.paginate(Bucket=bucket)
for page in page_iterator:
    all_data_files += [c["Key"] for c in page["Contents"]]

for file in all_data_files:
    #Read raw file
    obj = s3.get_object(Bucket=bucket, Key=file)
    raw_data = tab_raw_data(file, obj)

    i=1
    n = len(raw_data)
    for _, row in raw_data.iterrows():
        start = time.time()
        row = row.to_frame().transpose()
        #Store data_file in database
        col_dict = {"startTimeMillis": "start_time_millis",
                    "endTimeMillis": "end_time_millis",
                    "startTime": "start_time",
                    "endTime": "end_time",
                    "date_created": "date_created" ,
                    "date_updated": "date_updated",
                    "file_name": "file_name",
                    "json_hash": "json_hash",
                    }

        raw_data_tosql = row.rename(columns=col_dict)
        raw_data_tosql = raw_data_tosql[list(col_dict.values())]
        try:
            raw_data_tosql.to_sql(name="data_files", schema="waze", con=meta.bind, if_exists="append", index=False)
        except exc.IntegrityError:
            print("Data file is already stored in the relational database. Stopping process.")
            break

        #Introspect data_file table
        meta.reflect(schema="waze")
        data_files = meta.tables["waze.data_files"]
        datafile_result = (select([data_files.c.id]).where(data_files.c.json_hash == raw_data["json_hash"].iloc[0])
                                                   .execute()
                                                   .fetchall()
                          )
        if len(datafile_result) > 1:
            raise Exception("There should be only one result")

        datafile_id = datafile_result[0][0]

        #Store jams in database
        jams_tosql = tab_jams(row)
        if jams_tosql is not None:
            jams_tosql["datafile_id"] = datafile_id
            jams_tosql.to_sql(name="jams", schema="waze", con=meta.bind, if_exists="append", index=False,
                                  dtype={"line": typeJSON}
                                 )



        #Store alerts in database
        alerts_tosql = tab_alerts(row)
        if alerts_tosql is not None:
            alerts_tosql["datafile_id"] = datafile_id
            alerts_tosql.to_sql(name="alerts", schema="waze", con=meta.bind, if_exists="append", index=False,
                                  dtype={"location": typeJSON}
                                 )

        #Store irregularities in databse
        irregs_tosql = tab_irregularities(row)
        if irregs_tosql is not None:
            irregs_tosql["datafile_id"] = datafile_id
            irregs_tosql.to_sql(name="irregularities", schema="waze", con=meta.bind, if_exists="append", index=False,
                                  dtype={"line": typeJSON}
                                 )

        end = time.time()
        elapsed = str(int(end-start))

        print("Stored DataFile", str(i), "of", str(n),"from", file, "in", elapsed, "seconds.")
        i += 1
