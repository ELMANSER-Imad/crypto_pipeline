# Crypto Market Analysis Pipeline

## Objectif
Ce projet vise à analyser les données de marché des cryptomonnaies en utilisant un pipeline de données qui inclut l'ingestion, le stockage, le traitement, et l'analyse des données.

## Technologies Utilisées
- Apache Airflow
- Hadoop (HDFS, MapReduce)
- HBase
- CoinGecko API

## Installation
1. Clonez ce dépôt.
2. Installez les dépendances : `pip install -r requirements.txt`.
3. Configurez Airflow, Hadoop, et HBase selon les instructions fournies.

## Exécution
- Exécutez le DAG Airflow pour lancer le pipeline.
- Les données seront stockées dans HDFS et HBase.

## Exemples de Requêtes HBase
- Récupérer le prix moyen du Bitcoin le 2025-01-01 :
  ```bash
  get 'crypto_prices', 'bitcoin_2025-01-01'
