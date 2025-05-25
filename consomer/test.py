import json
import logging
import os
import time
from datetime import datetime
from hdfs import InsecureClient
from kafka import KafkaConsumer, KafkaProducer
from pyspark.sql import SparkSession
from pyspark.ml.classification import GBTClassificationModel
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import to_date, dayofmonth, dayofweek, month, when, col

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transaction_topic")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "fraud_topic")
HDFS_NAMENODE_URL = os.getenv("HDFS_NAMENODE_URL", "http://namenode:9870")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "5"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "5"))
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")

# Dynamic HDFS paths based on current date
today = datetime.today()
date_path = f"YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}"
HDFS_MODEL_PATH = f"/user/root/model/fraud_detection/{date_path}/gradient-boosted_trees"
HDFS_PREPROCESSOR_PATH = f"/user/root/model/fraud_detection/{date_path}/preprocessor"
LOCAL_MODEL_PATH = "/tmp/model"
LOCAL_PREPROCESSOR_PATH = "/tmp/preprocessor"

def create_spark_session():
    """Creates and returns a SparkSession with appropriate configuration."""
    try:
        logger.info("Initialisation de la session Spark...")
        spark = SparkSession.builder \
            .appName("FraudConsumer") \
            .master("local[*]") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .getOrCreate()
        logger.info("Session Spark initialisée avec succès")
        return spark
    except Exception as e:
        logger.error(f"Échec de l'initialisation de Spark : {str(e)}")
        raise

def define_schema():
    """Defines the schema for incoming transactions, excluding Is Fraudulent."""
    try:
        logger.info("Définition du schéma des données...")
        schema = StructType([
            StructField("Transaction ID", StringType(), True),
            StructField("Customer ID", StringType(), True),
            StructField("Transaction Amount", FloatType(), True),
            StructField("Transaction Date", StringType(), True),
            StructField("Payment Method", StringType(), True),
            StructField("Product Category", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("Customer Age", IntegerType(), True),
            StructField("Customer Location", StringType(), True),
            StructField("Device Used", StringType(), True),
            StructField("IP Address", StringType(), True),
            StructField("Shipping Address", StringType(), True),
            StructField("Billing Address", StringType(), True),
            StructField("Account Age Days", IntegerType(), True),
            StructField("Transaction Hour", IntegerType(), True)
        ])
        logger.info("Schéma des données défini avec succès")
        return schema
    except Exception as e:
        logger.error(f"Échec de la définition du schéma : {str(e)}")
        raise

def load_model_from_hdfs(spark):
    """Loads the GBT model and preprocessing pipeline from HDFS with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            logger.info(f"Tentative {attempt + 1} de connexion à HDFS...")
            hdfs_client = InsecureClient(HDFS_NAMENODE_URL, user="root")
            logger.info(f"Connecté à HDFS : {HDFS_NAMENODE_URL}")

            # Check and download model
            logger.info(f"Vérification de l'existence du modèle dans HDFS : {HDFS_MODEL_PATH}")
            model_status = hdfs_client.status(HDFS_MODEL_PATH, strict=True)
            logger.info(f"Modèle trouvé : {model_status}")
            logger.info(f"Téléchargement du modèle depuis {HDFS_MODEL_PATH} vers {LOCAL_MODEL_PATH}")
            hdfs_client.download(HDFS_MODEL_PATH, LOCAL_MODEL_PATH, overwrite=True)
            logger.info(f"Modèle téléchargé à {LOCAL_MODEL_PATH}")

            # Check and download preprocessor
            logger.info(f"Vérification de l'existence du pipeline dans HDFS : {HDFS_PREPROCESSOR_PATH}")
            preprocessor_status = hdfs_client.status(HDFS_PREPROCESSOR_PATH, strict=True)
            logger.info(f"Pipeline trouvé : {preprocessor_status}")
            logger.info(f"Téléchargement du pipeline depuis {HDFS_PREPROCESSOR_PATH} vers {LOCAL_PREPROCESSOR_PATH}")
            hdfs_client.download(HDFS_PREPROCESSOR_PATH, LOCAL_PREPROCESSOR_PATH, overwrite=True)
            logger.info(f"Pipeline téléchargé à {LOCAL_PREPROCESSOR_PATH}")

            # Load model and preprocessor
            logger.info("Chargement du modèle Spark ML...")
            model = GBTClassificationModel.load(LOCAL_MODEL_PATH)
            logger.info("Modèle chargé avec succès")
            logger.info("Chargement du pipeline de prétraitement...")
            preprocessor = PipelineModel.load(LOCAL_PREPROCESSOR_PATH)
            logger.info("Pipeline chargé avec succès")

            return model, preprocessor
        except Exception as e:
            logger.error(f"Tentative {attempt + 1} échouée : {str(e)}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Attente de {RETRY_DELAY} secondes avant la prochaine tentative...")
                time.sleep(RETRY_DELAY)
            else:
                logger.error("Échec du chargement du modèle après toutes les tentatives")
                return None, None

def preprocess_transaction(transaction, spark, schema, preprocessor):
    """Preprocesses a transaction using the loaded preprocessing pipeline."""
    try:
        # Validate input transaction
        required_cols = schema.fieldNames()
        transaction_id = transaction.get("Transaction ID", "inconnue")
        missing_cols = [col for col in required_cols if col not in transaction]
        if missing_cols:
            raise ValueError(f"Colonnes manquantes dans la transaction {transaction_id}: {missing_cols}")

        # Create DataFrame from transaction
        logger.info(f"Création du DataFrame pour la transaction {transaction_id}...")
        df = spark.createDataFrame([transaction], schema=schema)
        if df is None or df.rdd.isEmpty():
            raise ValueError("DataFrame vide après conversion du schéma")

        # Extract features from Transaction Date
        logger.info("Extraction des features depuis Transaction Date...")
        df = df.withColumn("Transaction Date", to_date(col("Transaction Date")))
        df = df.withColumn("DayOfMonth", dayofmonth(col("Transaction Date")))
        df = df.withColumn("DayOfWeek", dayofweek(col("Transaction Date")))
        df = df.withColumn("Month", month(col("Transaction Date")))
        logger.info("DayOfMonth, DayOfWeek, Month ajoutés avec succès")

        # Create derived features
        logger.info("Création des features dérivées...")
        df = df.withColumn("AddressMismatch", 
                          when(col("Shipping Address") != col("Billing Address"), 1).otherwise(0))
        df = df.withColumn("AmountPerQuantity", 
                          when(col("Quantity") > 0, col("Transaction Amount") / col("Quantity")).otherwise(col("Transaction Amount")))
        df = df.withColumn("IsWeekend", 
                          when((col("DayOfWeek") == 1) | (col("DayOfWeek") == 7), 1).otherwise(0))
        df = df.withColumn("IsNightTime", 
                          when((col("Transaction Hour") >= 22) | (col("Transaction Hour") <= 5), 1).otherwise(0))
        logger.info("Features dérivées ajoutées avec succès")

        # Handle null values in categorical columns
        categorical_cols = ["Payment Method", "Product Category", "Customer Location", "Device Used"]
        for col_name in categorical_cols:
            df = df.fillna("unknown", subset=[col_name])
        logger.info("Valeurs nulles dans les colonnes catégoriques remplies par 'unknown'")

        # Log schema before preprocessing
        logger.info(f"Schéma avant prétraitement : {df.columns}")

        # Apply preprocessing pipeline
        logger.info("Application du pipeline de prétraitement...")
        processed_df = preprocessor.transform(df)
        logger.info("Pipeline appliqué avec succès")

        return processed_df
    except Exception as e:
        logger.error(f"Erreur lors du prétraitement de la transaction {transaction_id} : {str(e)}")
        return None

def predict_fraud(transaction_df, model, spark):
    """Predicts fraud using the loaded model."""
    if transaction_df is None or transaction_df.rdd.isEmpty():
        logger.warning("DataFrame vide, impossible de faire une prédiction")
        return False
    try:
        logger.info("Prédiction avec le modèle...")
        prediction = model.transform(transaction_df)
        is_fraud = prediction.select("prediction").first()[0]
        logger.info(f"Prédiction obtenue : Fraude = {bool(is_fraud)}")
        return bool(is_fraud)
    except Exception as e:
        logger.error(f"Erreur lors de la prédiction avec le modèle : {str(e)}")
        return False

def main():
    """Main function to consume transactions, predict fraud, and produce results."""
    spark = None
    try:
        spark = create_spark_session()
        schema = define_schema()
        model, preprocessor = load_model_from_hdfs(spark)
        if model is None or preprocessor is None:
            logger.error("Échec du chargement du modèle ou du pipeline")
            return

        logger.info("Initialisation du consommateur et producteur Kafka...")
        consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset=AUTO_OFFSET_RESET,
            group_id='fraud_detector',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            session_timeout_ms=60000,
            heartbeat_interval_ms=20000,
            max_poll_interval_ms=600000
        )
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks='all',
            retries=5,
            retry_backoff_ms=1000
        )
        logger.info(f"Consommateur Kafka démarré pour le topic {INPUT_TOPIC}")

        for message in consumer:
            try:
                transaction = message.value
                transaction_id = transaction.get("Transaction ID", "inconnue")
                logger.info(f"Transaction reçue : {transaction_id}")
                
                # Remove Is Fraudulent if present
                transaction.pop("Is Fraudulent", None)
                
                processed_df = preprocess_transaction(transaction, spark, schema, preprocessor)
                if processed_df is None:
                    logger.warning(f"Transaction {transaction_id} ignorée (erreur de prétraitement)")
                    continue  # Skip sending result to avoid marking as non-fraudulent
                else:
                    is_fraud = predict_fraud(processed_df, model, spark)
                    result = {
                        "transaction_id": transaction_id,
                        "is_fraud": is_fraud,
                        "timestamp": transaction.get("Transaction Date", None),
                        "error": None
                    }
                
                producer.send(OUTPUT_TOPIC, result)
                producer.flush()
                logger.info(f"Transaction {transaction_id} traitée - Fraude={result['is_fraud']}")
            except Exception as e:
                logger.error(f"Erreur lors du traitement de la transaction {transaction_id} : {str(e)}")
                continue  # Skip sending error result
    except Exception as e:
        logger.error(f"Erreur fatale dans la boucle principale : {str(e)}")
    finally:
        if spark:
            spark.stop()
            logger.info("Session Spark arrêtée")

if __name__ == "__main__":
    main()