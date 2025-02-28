from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import subprocess
import pandas as pd
import happybase

default_args = {
    'owner': 'etudiant',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_coingecko_data(**context):
    """Récupère les données depuis l'API CoinGecko."""
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        'vs_currency': 'usd',
        'order': 'market_cap_desc',
        'per_page': '100',
        'page': '1',
        'sparkline': 'false'
    }
    response = requests.get(url, params=params)
    data = response.json()
    context['ti'].xcom_push(key='raw_data', value=data)

def store_raw_data_in_hdfs(**context):
    """Stocke les données brutes dans HDFS avec partition par date."""
    data = context['ti'].xcom_pull(key='raw_data')
    json_data = json.dumps(data)
    local_file = '/tmp/coingecko_raw.json'
    with open(local_file, 'w') as f:
        f.write(json_data)
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/raw/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/coingecko_raw.json"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file_path])

def process_data(**context):
    """Transforme et agrège les données."""
    data = context['ti'].xcom_pull(key='raw_data')
    df = pd.DataFrame(data)
    df['price_avg'] = df['current_price'].mean()
    df['price_min'] = df['current_price'].min()
    df['price_max'] = df['current_price'].max()
    df['volume_avg'] = df['total_volume'].mean()
    processed_data = df.to_dict(orient='records')
    context['ti'].xcom_push(key='processed_data', value=processed_data)

def store_processed_data_in_hdfs(**context):
    """Stocke les données traitées dans HDFS."""
    data = context['ti'].xcom_pull(key='processed_data')
    df = pd.DataFrame(data)
    local_file = '/tmp/coingecko_processed.parquet'
    df.to_parquet(local_file)
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/etudiant/crypto/processed/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/coingecko_processed.parquet"
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", hdfs_dir])
    subprocess.run(["hdfs", "dfs", "-put", "-f", local_file, hdfs_file_path])

def load_data_into_hbase(**context):
    """Charge les données traitées dans HBase."""
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_file_path = f"/user/etudiant/crypto/processed/YYYY={year}/MM={month}/DD={day}/coingecko_processed.parquet"
    
    # Télécharger le fichier Parquet depuis HDFS vers un répertoire local temporaire
    local_file = '/tmp/coingecko_processed.parquet'
    subprocess.run(["hdfs", "dfs", "-get", hdfs_file_path, local_file])
    
    # Lire le fichier Parquet
    df = pd.read_parquet(local_file)
    
    # Connexion à HBase
    connection = happybase.Connection('localhost')
    table = connection.table('crypto_data')
    
    # Charger les données dans HBase
    for index, row in df.iterrows():
        row_key = f"{row['id']}_{execution_date}"
        table.put(row_key, {
            b'cf:price_avg': str(row['price_avg']).encode(),
            b'cf:price_min': str(row['price_min']).encode(),
            b'cf:price_max': str(row['price_max']).encode(),
            b'cf:volume_avg': str(row['volume_avg']).encode(),
        })

with DAG(
    'crypto_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_data',
        python_callable=fetch_coingecko_data,
        provide_context=True
    )

    store_raw_data = PythonOperator(
        task_id='store_raw_data_in_hdfs',
        python_callable=store_raw_data_in_hdfs,
        provide_context=True
    )

    process_data = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True
    )

    store_processed_data = PythonOperator(
        task_id='store_processed_data_in_hdfs',
        python_callable=store_processed_data_in_hdfs,
        provide_context=True
    )

    load_data = PythonOperator(
        task_id='load_data_into_hbase',
        python_callable=load_data_into_hbase,
        provide_context=True
    )

    fetch_data >> store_raw_data >> process_data >> store_processed_data >> load_data