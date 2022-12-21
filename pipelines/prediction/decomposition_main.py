import os
import sys
import uuid
import datetime
from configparser import ConfigParser
from math import ceil
import ray

# set path
project_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_path)

from pipelines.prediction.decompose.decomposer import Decomposition
from utils.logs.log import standardLog
from utils.result import Reporting
standardLog = standardLog()
reporting = Reporting(job='Time Series Decomposition')

# config
config = ConfigParser()
config.read(os.path.join(project_path, 'config.ini')) 

# data path
data_path = config.get('Preprocessing', 'DATA_DIR')

# save path
save_path = project_path + '/data/decompose_data'
if not os.path.exists(save_path):
    os.makedirs(save_path, exist_ok=True)


providers = ['pod', 'vm']
metrics = {"vm":[ "cpu", "diskio-write", "diskio-read","memory","network-in", "network-out","filesystem"],
            "pod":["cpu", "memory"]}

metric_col = {'cpu': 'mean_cpu_usage',
            'memory' : 'mean_memory_usage',
            'network-in' : 'mean_network_in_bytes',
            'network-out' : 'mean_network_out_bytes',
            'diskio-read' : 'mean_read_bytes',
            'diskio-write' : 'mean_write_bytes',
            'filesystem' : 'mean_filesystem_usage'
            }