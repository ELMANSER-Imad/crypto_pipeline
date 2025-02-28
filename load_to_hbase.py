import subprocess
import happybase
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime
import logging
import traceback

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Parquet to HBase") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Connexion à HBase
try:
    connection = happybase.Connection('localhost')
    table = connection.table('crypto_prices')
    logging.info("✅ Connexion à HBase réussie")
except Exception as e:
    logging.error(f"❌ Erreur de connexion à HBase : {e}")
    exit(1)

# Chemin de base des données HDFS
base_path = "/user/etudiant/crypto/processed/YYYY=2025"

# Fonction pour lister les fichiers/dossiers dans HDFS
def list_hdfs_files(path):
    """ Liste les fichiers ou dossiers présents dans HDFS à l'aide de `hdfs dfs -ls` """
    try:
        result = subprocess.run(["hdfs", "dfs", "-ls", path], capture_output=True, text=True)
        if result.returncode != 0:
            logging.warning(f"⚠️ Aucun fichier trouvé dans {path}")
            return []
        
        lines = result.stdout.strip().split("\n")[1:]  # Ignorer la première ligne (total X)
        return [line.split()[-1] for line in lines]  # Extraire le chemin du fichier/dossier
    except Exception as e:
        logging.error(f"❌ Erreur lors de la lecture de {path} : {e}")
        return []

# Fonction pour traiter un fichier Parquet et l'insérer dans HBase
def process_parquet_file(file_path, date):
    try:
        # Vérifier si le fichier existe dans HDFS
        result = subprocess.run(["hdfs", "dfs", "-test", "-e", file_path], capture_output=True)
        if result.returncode != 0:
            logging.warning(f"⚠️ Fichier non trouvé : {file_path}")
            return

        # Lire le fichier Parquet depuis HDFS
        df = spark.read.parquet(file_path)

        # Vérifier que les colonnes nécessaires sont présentes
        required_columns = {"coin_id", "price", "volume"}
        if not required_columns.issubset(set(df.columns)):
            logging.warning(f"⚠️ Fichier {file_path} ne contient pas toutes les colonnes requises {required_columns}")
            return
        
        # Afficher le nom du fichier et le nombre de lignes
        logging.info(f"📂 Traitement du fichier : {file_path}")
        logging.info(f"📊 Nombre de lignes : {df.count()}")

        # Transformer les données (agrégation)
        aggregated_df = df.groupBy("coin_id").agg(
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.avg("price").alias("avg_price"),
            F.sum("volume").alias("total_volume")
        )

        # Convertir le DataFrame Spark en Pandas pour l'insertion dans HBase
        pandas_df = aggregated_df.toPandas()

        # Vérifier si le DataFrame est vide
        if pandas_df.empty:
            logging.warning(f"⚠️ Aucune donnée à insérer pour {file_path}")
            return

        # Insérer les données dans HBase avec un batch pour optimiser
        with table.batch(batch_size=1000) as batch:
            for _, row in pandas_df.iterrows():
                row_key = f"{row['coin_id']}_{date}".encode()
                batch.put(row_key, {
                    b'stats:price_min': str(row['min_price']).encode(),
                    b'stats:price_max': str(row['max_price']).encode(),
                    b'stats:price_avg': str(row['avg_price']).encode(),
                    b'stats:volume_sum': str(row['total_volume']).encode()
                })

        logging.info(f"✅ Données insérées dans HBase pour {file_path}")

    except Exception as e:
        logging.error(f"❌ Erreur lors du traitement de {file_path} : {e}")
        traceback.print_exc()  # Afficher la stack trace pour le débogage

# Vérifier si le répertoire de base existe
if not list_hdfs_files(base_path):
    logging.error(f"❌ Aucun fichier trouvé dans le répertoire de base : {base_path}")
    exit(1)

# Parcourir les mois (MM=01, MM=02, etc.)
for month_path in list_hdfs_files(base_path):
    month = month_path.split("=")[-1]  # Extraire MM

    # Parcourir les jours (DD=01, DD=02, etc.)
    for day_path in list_hdfs_files(month_path):
        day = day_path.split("=")[-1]  # Extraire DD

        # Récupérer la date (YYYY-MM-DD)
        try:
            date = datetime.strptime(f"2025-{month}-{day}", "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            logging.error(f"❌ Format de date invalide pour {month}-{day}")
            continue

        # Parcourir les fichiers Parquet dans HDFS
        for file_path in list_hdfs_files(day_path):
            logging.info(f"📄 Fichier trouvé : {file_path}")
            if file_path.endswith(".parquet"):
                process_parquet_file(file_path, date)

# Fermer la connexion HBase et arrêter Spark
try:
    connection.close()
    spark.stop()
    logging.info("✅ Fin du traitement")
except Exception as e:
    logging.error(f"❌ Erreur lors de la fermeture des ressources : {e}")