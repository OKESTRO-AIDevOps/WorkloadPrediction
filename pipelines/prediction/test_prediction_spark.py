###예측잡이 실행되는 메인파일
import os
import sys
from configparser import ConfigParser
import socket
import time

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import ArrayType, DoubleType, StructType, StructField, StringType, TimestampType
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import pandas_udf, PandasUDFType, lit, when
import pandas as pd
import pymysql


from dependencies.elastic_spark import ElasticSpark
from prediction_models_spark import predict_prophet
from prediction_models_spark import predict_arima
from prediction_models_spark import predict_autoreg

project_path = os.path.dirname(os.path.dirname(__file__))

#config setup
if socket.gethostname() == 'bigdata-hdfs-spark-0-3':
    config = ConfigParser()
    config.read(project_path+'/../config.ini')
    config.set('SPARK','MASTER',"local[8]")
else:
    config = ConfigParser()
    config.read('config.ini')

#hadoop setup
hdfs_host, hdfs_port, hdfs_id, hdfs_pw = config.get('HDFS', 'HOST'), config.get('HDFS', 'PORT'),  config.get('HDFS', 'USER'), config.get('HDFS', 'PASSWORD')

#mariadb setup
db_host, db_port, db_id, db_pw, db_schema = config.get('MARIADB', 'HOST'), config.get('MARIADB', 'PORT'),  config.get('MARIADB', 'USER'), config.get('MARIADB', 'PASSWORD'), config.get('MARIADB','DATABASE')
db = pymysql.connect(host=db_host,port=int(db_port), user= db_id, passwd=db_pw, db= db_schema, charset='utf8')

def retrieve_machine_to_predict():
    """
    예측할 머신들을 가져오는 함수 ex) vm, pod
    :return:
    """
    return ["vm"]

def retrieve_model_to_predict(db):
    """
    예측할 모델을 가져오는 함수
    metadata api 를 통해 구현

    :param config: metadata 저장 config
    :return:
    model to predict : str
    """
    sql = "SELECT * FROM T_ALGORITHM"
    df=pd.read_sql(sql,db)
    df = df[df['USG_AT']=='Y'][['PVDR_ID','USG_AT','ALGORITHM_NM']]
    model_dict = {}
    for index, row in df.iterrows():
        machine , model = row['PVDR_ID'], row['ALGORITHM_NM']
        machine = 'vm' if 'openstack' in machine else 'pod'
        model_dict[machine]= model
    try:
        model_dict['vm']
        model_dict['pod']
        return model_dict
    except:
        return {'vm':'Prophet','pod':'Prophet'}

def retrieve_selected_model(model_name):
    """
    정해진 모델에 따라 model 함수 반환
    :param model_name:
    :return:
    """
    model = predict_prophet
    if model_name == 'Prophet':
        model = predict_prophet
    if model_name == 'Auto arima':
        model = predict_arima
    if model_name == 'Autoreg':
        model = predict_autoreg
    return model


def retrieve_metrics_to_predict():
    """
    예측을 할 변수들을 가져오는 역할을 하는 함수
    :return: list with variables to predict
    """
    # return {"vm": ["cpu","memory","network-in", "network-out", "diskio-write", "diskio-read"],
    #         "pod": ["cpu", "memory"]}
    return {"vm": ["cpu"], "pod":[]}

def retrieve_df_from_csv(spark_session, path):
    """
    예측에서 자제적으로 수행한 전처리 데이터를 가져오는 함수
    :param spark_session: spark session 객체
    :param path: csv 파일경로
    :return: df with columns host_id, timestamp, avg(value)
    """
    df = spark_session.read \
        .option("header", True) \
        .format("csv").load(path)
    return df