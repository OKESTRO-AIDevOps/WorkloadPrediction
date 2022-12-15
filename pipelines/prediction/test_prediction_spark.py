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