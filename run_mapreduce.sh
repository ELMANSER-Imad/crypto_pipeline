#!/bin/bash

# Chemin vers le fichier JAR de Hadoop Streaming
HADOOP_STREAMING_JAR="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.3.6.jar"

# Répertoire d'entrée dans HDFS
INPUT_DIR="/user/etudiant/crypto/raw/YYYY=2025/MM=01/DD=01/coingecko_raw.json"

# Répertoire de sortie dans HDFS
OUTPUT_DIR="/user/etudiant/crypto/processed/YYYY=2025/MM=01/DD=01"

# Chemins vers les scripts Mapper et Reducer
MAPPER_PATH="/home/ubuntu/airflow/mapper.py"
REDUCER_PATH="/home/ubuntu/airflow/reducer.py"

# Commande Hadoop Streaming
hadoop jar $HADOOP_STREAMING_JAR \
  -input $INPUT_DIR \
  -output $OUTPUT_DIR \
  -mapper $MAPPER_PATH \
  -reducer $REDUCER_PATH \
  -file $MAPPER_PATH \
  -file $REDUCER_PATH