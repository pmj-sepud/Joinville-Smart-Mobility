import os
import sys
project_dir = os.path.join(os.path.dirname(__file__), os.pardir, os.pardir)
sys.path.append(project_dir)

import dotenv
import time
import pandas as pd
import geopandas as gpd
from sqlalchemy import MetaData, create_engine, extract, select
from sqlalchemy.engine.url import URL
from sqlalchemy.sql import or_
from datetime import datetime, timedelta, date

from src.database.functions import build_df_trechos
from functions import get_direction, UTM_to_lon_lat

dotenv_path = os.path.join(project_dir, '.env')
dotenv.load_dotenv(dotenv_path)

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

jpt = meta.tables["JamPerTrecho"]
jam = meta.tables["Jam"]
trch = meta.tables["Trecho"]
trchAtt = meta.tables["TrechoAttributes"]
classFunc = meta.tables["ClassFunc"]
mongo_record = meta.tables["MongoRecord"]

hoje = datetime.now()
dias = 30
data_inicio = date(2017, 11, 25)
data_fim = date(2017, 12, 1)
hora_inicio_manha=9
hora_fim_manha=11
hora_inicio_tarde = 19
hora_fim_tarde = 21
dias_semana = [1,2,3,4,5]

#Número total de data points do Waze
start = time.time()
query = mongo_record.select().\
                where(mongo_record.c.MgrcDateStart.between(data_inicio, data_fim)).\
                where(or_(extract("hour", mongo_record.c.MgrcDateStart).between(hora_inicio_manha, hora_fim_manha),
                          extract("hour", mongo_record.c.MgrcDateStart).between(hora_inicio_tarde, hora_fim_tarde))).\
                where(extract("isodow", mongo_record.c.MgrcDateStart).in_(dias_semana))

df_records = pd.read_sql(query, meta.bind, index_col="MgrcId")
total_observations = len(df_records)

#Geotrechos
df_trechos = build_df_trechos(meta)
crs = "+proj=utm +zone=22J, +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
geo_trechos = gpd.GeoDataFrame(df_trechos, crs=crs, geometry="trecho_LineString")
geo_trechos = geo_trechos.to_crs({'init': 'epsg:4326'})

#Build df_jpt
query = select([jpt.c.JptId,
                jpt.c.JamDateStart,
                jpt.c.JamUuid,
                trch.c.TrchId,
                jam.c.JamId,
                jam.c.JamDscStreet,
                jam.c.JamIndLevelOfTraffic,
                jam.c.JamQtdLengthMeters,
                jam.c.JamSpdMetersPerSecond,
                jam.c.JamTimeDelayInSeconds,
                jam.c.JamQtdLengthMeters,
                jam.c.JamDscCoordinatesLonLat,
                trch.c.TrchDscNome,
                trch.c.TrchQtdComprimento,
                trch.c.TrchDscCoordxUtmComeco,
                trch.c.TrchDscCoordyUtmComeco,
                classFunc.c.ClfuDscClassFunc]).\
                select_from(jpt.join(jam).join(trch).join(trchAtt.join(classFunc))).\
                where(jam.c.JamDateStart.between(data_inicio, data_fim)).\
                where(or_(extract("hour", jam.c.JamDateStart).between(hora_inicio_manha, hora_fim_manha),
                          extract("hour", jam.c.JamDateStart).between(hora_inicio_tarde, hora_fim_tarde))).\
                where(extract("isodow", jam.c.JamDateStart).in_(dias_semana))

df_jpt = pd.read_sql(query, meta.bind, index_col="JptId")
df_jpt[["LonDirection","LatDirection"]] = df_jpt["JamDscCoordinatesLonLat"].apply(get_direction)
df_jpt.to_csv(project_dir+"/data/interim/df_jpt_" + \
                            data_inicio.strftime("%Y%m%d") + "_to_" + \
                            data_fim.strftime("%Y%m%d") + ".csv"
              )
end = time.time()
processing_time = round(end-start)
minutos_engarrafados = df_jpt["JamId"].nunique()
n_ruas = df_jpt["TrchDscNome"].nunique()
n_trechos = df_jpt["TrchId"].nunique()

print("De "+ str(data_inicio) + " a " + str(data_fim) + " das " + str(hora_inicio_manha-2) +
      " às " + str(hora_fim_manha-1) + " e das " + str(hora_inicio_tarde-2) + " às " + str(hora_fim_tarde-1) + ".")

dict_dias_semana = {1: "Segunda-feira",
              2: "Terça-feira",
              3: "Quarta-feira",
              4: "Quinta-feira",
              5: "Sexta-feira",
              6: "Sábado",
              7: "Domingo",}

string_dias = [dict_dias_semana[i] for i in dias_semana]
string = ", ".join(string_dias)

print("Dias da semana: " + string + ".")
print("Tempo para carregamento dos dados: " + str(processing_time) + " segundos.")
print("Minutos de engarrafamento carregados: " + str(minutos_engarrafados))
print("Número de ruas abrangidas: " + str(n_ruas))
print("Número de trechos abrangidos: " + str(n_trechos))
columns = {"TrchDscNome": "Rua",
           "TrchId": "Trecho",
           "JamDateStart": "Data (GMT-3)",
           "JamQtdLengthMeters": "Comprimento da fila (m)",
           "JamSpdMetersPerSecond": "Velocidade (km/h)",
           "JamTimeDelayInSeconds": "Atraso (s)",
           "JamIndLevelOfTraffic": "Nível de trânsito (0 a 5)",
          }

df_jpt_toshow = df_jpt.rename(columns=columns)
df_jpt_toshow["Velocidade (km/h)"] = df_jpt_toshow["Velocidade (km/h)"]*3.6
df_jpt_toshow[[c for c in columns.values()]].sample(7).sort_values("Data (GMT-3)", ascending=False)


"""
    def test_normalize_jpt(self):

        jpt_to_normalize = pd.read_csv("../test_data/test_jpt_to_normalize.csv", index_col=0)
        jpt_to_normalize["JamDateStart"] = jpt_to_normalize["JamDateStart"].astype('datetime64[ns]')

        norm_df_jpt = normalize_jpt(jpt_to_normalize)

        #import pdb
        #pdb.set_trace()

        test_date = date(year=2017, month=11, day=25)

        self.assertEqual(norm_df_jpt.shape, (2,10))
        self.assertEqual(norm_df_jpt[norm_df_jpt["JamDateStart"] < test_date]["JamIndLevelOfTraffic"].iloc[0], 2)


    def test_get_pivot_jpt_means(self):
        norm_df_jpt = pd.read_csv("../test_data/test_norm_df_jpt.csv")

        pivot_jpt_means = get_pivot_jpt_means(norm_df_jpt)

        self.assertEqual(pivot_jpt_means.shape, (1,6))
        self.assertEqual(pivot_jpt_means["JamIndLevelOfTraffic"].iloc[0], 3)


    def test_get_pivot_jpt_count(self):
        norm_df_jpt = pd.read_csv("../test_data/test_norm_df_jpt.csv")

        pivot_jpt_count = get_pivot_jpt_count(norm_df_jpt)

        self.assertEqual(pivot_jpt_count.shape, (1,1))
        self.assertEqual(pivot_jpt_count["JamDateStart"].iloc[0], 2)

    def test_gen_pivot_table(self):

        df_jpt = pd.read_csv("../test_data/test_jpt_to_normalize.csv")

        total_observations = df_jpt['JamDateStart'].nunique()

        pivot_table = gen_pivot_table(df_jpt, total_observations)

        self.assertEqual(pivot_table.shape, (1,9))
        self.assertEqual(pivot_table.iloc[0]["Percentual de trânsito (min engarrafados / min monitorados)"],1)
"""
