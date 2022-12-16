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