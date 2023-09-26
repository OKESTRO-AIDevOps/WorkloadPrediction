import os
import uuid
import datetime
from configparser import ConfigParser
import ray
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from itertools import product

# Import your custom modules here

# Initialize ray
ray.init()

# Define constants and configurations
project_path = Path(__file__).resolve().parent.parent.parent
config = ConfigParser()
config.read(project_path / 'config.ini')

data_path = config.get('Preprocessing', 'DATA_DIR')
save_path = project_path / 'data' / 'decompose_data'
save_path.mkdir(parents=True, exist_ok=True)

providers = ['pod', 'vm']
metrics = {
    "vm": ["cpu", "diskio-write", "diskio-read", "memory", "network-in", "network-out", "filesystem"],
    "pod": ["cpu", "memory"]
}

metric_col = {
    'cpu': 'mean_cpu_usage',
    'memory': 'mean_memory_usage',
    'network-in': 'mean_network_in_bytes',
    'network-out': 'mean_network_out_bytes',
    'diskio-read': 'mean_read_bytes',
    'diskio-write': 'mean_write_bytes',
    'filesystem': 'mean_filesystem_usage'
}


# Define a function for the decomposition process
def decompose_data(decomposition_id, provider, metric):
    try:
        decomposition_id = str(datetime.datetime.now())[:10] + str(uuid.uuid1())
        print(f'Decomposition analysis ID : {decomposition_id}')
        print(f'===============Decompose PROVIDER : {provider} METRIC : {metric}===============')
        metric_to_read = metric.split("-")[0] if provider == 'vm' else 'pod'

        decomposer = Decomposition(
            decomposition_id=decomposition_id,
            provider=provider,
            metric_to_read=metric_to_read,
            metric=metric,
            feature=metric_col[metric],
            data_path=data_path,
            save_path=save_path,
            duration=3,
            period='M',
            agg_interval='5m',
            duration_threshold=3,
            seasonal_threshold=0.4
        )

        df = decomposer.read_data()
        decomposer.analysis_ts()

        print(f'Host : {len(decomposer.hosts)}')
        for p in decomposer.periods:
            print(f'--------------------------------decompose by {p}--------------------------------')
            decomp_period = decomposer.period_dict[p] * decomposer.duration_threshold
            smoother_len = decomp_period + 1 if decomp_period % 2 == 0 else decomp_period
            start = 0
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(decomposer.decompose_host, df, i, p, smoother_len) for i in
                           range(start, len(decomposer.hosts))]
                ray.get(futures)

    except Exception as e:
        print(f'Error: {e}')


# Entry point for the script
if __name__ == '__main__':
    start_time = datetime.datetime.now(datetime.timezone.utc)

    # Create a product of providers and metrics to iterate through
    provider_metric_combinations = list(product(providers, metrics.keys()))

    # Iterate through provider-metric combinations and parallelize using ThreadPoolExecutor
    with ThreadPoolExecutor() as executor:
        for provider, metric in provider_metric_combinations:
            executor.submit(decompose_data, provider, metric)

    end_time = datetime.datetime.now(datetime.timezone.utc)
    print(f'start : {start_time}, \n end: {end_time}')