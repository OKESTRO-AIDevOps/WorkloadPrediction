###예측잡이 실행되는 메인파일
import os
import sys
from configparser import ConfigParser
import socket
import time

import ray

import pandas as pd
import numpy as np
import pymysql

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

project_path = os.path.dirname(os.path.dirname(__file__))
from prediction_ray_models import predict_prophet
from prediction_ray_models import predict_arima
from prediction_ray_models import predict_autoreg
from prediction_ray_models import bulk_index_by_ray


#config setup
if socket.gethostname() == 'bigdata-hdfs-spark-0-3':
    config = ConfigParser()
    config.read(project_path+'/../config.ini')
    config.set('SPARK','MASTER',"local[8]")
else:
    config = ConfigParser()
    config.read('config.ini')


#mariadb setup
db_host, db_port, db_id, db_pw, db_schema = config.get('MARIADB', 'HOST'), config.get('MARIADB', 'PORT'),  config.get('MARIADB', 'USER'), config.get('MARIADB', 'PASSWORD'), config.get('MARIADB','DATABASE')
db = pymysql.connect(host=db_host,port=int(db_port), user= db_id, passwd=db_pw, db= db_schema, charset='utf8')

#es init
es_host=config.get('ES','HOST')
es_port=config.get('ES','PORT')
es_id=config.get('ES','USER')
es_pw=config.get('ES','PASSWORD')
es_info = [es_host,es_port,es_id,es_pw]
es = Elasticsearch(hosts=f"http://{es_id}:{es_pw}@{es_host}:{es_port}/", timeout= 100, http_compress=False)


ray.init(num_cpus=2, dashboard_host ='0.0.0.0')

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