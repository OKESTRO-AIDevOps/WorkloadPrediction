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


if socket.gethostname() == 'bigdata-hdfs-spark-0-3':
    config = ConfigParser()
    config.read(project_path+'/../config.ini')
    config.set('SPARK','MASTER',"local[8]")
else:
    config = ConfigParser()
    config.read('config.ini')


db_host, db_port, db_id, db_pw, db_schema = config.get('MARIADB', 'HOST'), config.get('MARIADB', 'PORT'),  config.get('MARIADB', 'USER'), config.get('MARIADB', 'PASSWORD'), config.get('MARIADB','DATABASE')
db = pymysql.connect(host=db_host,port=int(db_port), user= db_id, passwd=db_pw, db= db_schema, charset='utf8')

es_host=config.get('ES','HOST')
es_port=config.get('ES','PORT')
es_id=config.get('ES','USER')
es_pw=config.get('ES','PASSWORD')
es_info = [es_host,es_port,es_id,es_pw]
es = Elasticsearch(hosts=f"http://{es_id}:{es_pw}@{es_host}:{es_port}/", timeout= 100, http_compress=False)


ray.init(num_cpus=2, dashboard_host ='host')

def retrieve_machine_to_predict():
    return ["vm"]

def retrieve_model_to_predict(db):
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
    model = predict_prophet
    if model_name == 'Prophet':
        model = predict_prophet
    if model_name == 'Auto arima':
        model = predict_arima
    if model_name == 'Autoreg':
        model = predict_autoreg
    return model


def retrieve_metrics_to_predict():
    return {"vm":["cpu"],"pod":[]}

def retrieve_df_from_csv(path):
    df = pd.read_csv(path)
    return df

def retrieve_df_from_parquet():
    pass

def change_df_column_names(df, metric):
    if metric == 'cpu':
        df.rename(columns={'datetime':'timestamp', 'mean_cpu_usage':'avg(value)', 'host_name':'host_id'}, inplace=True)
    if metric == 'memory':
        df.rename(columns={'datetime':'timestamp', 'mean_memory_usage':'avg(value)', 'host_name':'host_id'}, inplace=True)
    if metric == 'network-in':
        df.rename(columns={'datetime':'timestamp', 'mean_network_in_bytes':'avg(value)', 'host_name':'host_id'}, inplace=True)
    if metric == 'network-out':
        df.rename(columns={'datetime':'timestamp', 'mean_network_out_bytes':'avg(value)', 'host_name':'host_id'}, inplace=True)
    if metric == 'diskio-read':
        df.rename(columns={'datetime':'timestamp', 'mean_read_bytes':'avg(value)', 'host_name':'host_id'}, inplace=True)
    if metric == 'diskio-write':
        df.rename(columns={'datetime':'timestamp', 'mean_write_bytes':'avg(value)', 'host_name':'host_id'}, inplace=True)

    return df

def main():
    model_dict = retrieve_model_to_predict(db)
    metric_dict = retrieve_metrics_to_predict()
    print(f"model_dict: {model_dict} , metric_dict: {metric_dict} ")
    metrics_to_predict = retrieve_metrics_to_predict()
    for machine in retrieve_machine_to_predict():
        model_selected_by_machine = model_dict[machine]
        metrics_selected_by_machine = metric_dict[machine]
        for metric in metrics_selected_by_machine:
            start1 = time.time()
            metric_to_read = metric.split("-")[0] if machine == 'vm' else 'pod'
            df = retrieve_df_from_csv(f"path/your/csv/{metric_to_read}.csv")
            print("df.shape:", df.shape)
            df = change_df_column_names(df, metric)
            hosts = np.unique(df['host_id'])
            print("number hosts:", len(hosts))
            put_dataset = ray.put(df)
            model = retrieve_selected_model(model_selected_by_machine)
            start2 = time.time()
            end1 = time.time()
            futures = ray.get([model.remote(idx, put_dataset, hosts, metric) for idx in range(len(hosts))])
            end2 = time.time()
            start3 =time.time()
            es_index = f'aiplatform-metric-{machine}-prediction-{metric}-ray-test'
            es.indices.delete(index=es_index, ignore=[400, 404])
            es.indices.create(index=es_index, body={})
            start4 = time.time()
            put_futures = ray.put(futures)
            ray.get([bulk_index_by_ray.remote(idx, put_futures, es_index, es_info) for idx in range(len(hosts))])
            end4 =time.time()
            start5 =time.time()
            end5 = time.time()
            end3 = time.time()
            print(f"time to store:{end3-start3}")
            print(f"model elapsed_time:{end2-start2}")
            print(f"preprocessed elapsed_time:{end1-start1}")
            print(f"pd concat elapsed_time:{end4-start4}")
            print(f"bulk elapsed_time:{end5-start5}")


if __name__ == '__main__':
    main()