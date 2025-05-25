import json
import logging
import os
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "fraud_detection")
INPUT_TOPIC = os.getenv("INPUT_TOPIC", "transaction_topic")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC", "fraud_topic")
AUTO_OFFSET_RESET = os.getenv("AUTO_OFFSET_RESET", "latest")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))

def connect_to_cassandra():
    """Connects to Cassandra and returns a session."""
    try:
        logger.info("Connexion à Cassandra...")
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connexion à Cassandra établie avec succès")
        return session
    except Exception as e:
        logger.error(f"Échec de la connexion à Cassandra : {str(e)}")
        raise

def prepare_statements(session):
    """Prepares insert statements for transactions and fraud results."""
    try:
        transaction_insert = session.prepare("""
            INSERT INTO fraud_detection.transactions (
                transaction_id, customer_id, transaction_amount, transaction_date,
                payment_method, product_category, quantity, customer_age,
                customer_location, device_used, ip_address, shipping_address,
                billing_address, account_age_days, transaction_hour, is_fraudulent
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """)
        fraud_insert = session.prepare("""
            INSERT INTO fraud_detection.fraud_results (
                transaction_id, is_fraud, timestamp, amount,
                payment_method, product_category, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """)
        logger.info("Instructions d'insertion préparées avec succès")
        return transaction_insert, fraud_insert
    except Exception as e:
        logger.error(f"Échec de la préparation des instructions : {str(e)}")
        raise

def process_transaction_batch(session, transactions, transaction_insert):
    """Processes a batch of transactions and inserts into Cassandra."""
    batch = BatchStatement()
    for transaction in transactions:
        transaction_id = transaction.get("Transaction ID", "unknown")
        try:
            # Convert transaction_date to datetime
            transaction_date = datetime.strptime(
                transaction.get("Transaction Date", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "%Y-%m-%d %H:%M:%S"
            )
            batch.add(transaction_insert, (
                transaction_id,
                transaction.get("Customer ID", "unknown"),
                float(transaction.get("Transaction Amount", 0.0)),
                transaction_date,
                transaction.get("Payment Method", "unknown"),
                transaction.get("Product Category", "unknown"),
                int(transaction.get("Quantity", 0)),
                int(transaction.get("Customer Age", 0)),
                transaction.get("Customer Location", "unknown"),
                transaction.get("Device Used", "unknown"),
                transaction.get("IP Address", "unknown"),
                transaction.get("Shipping Address", "unknown"),
                transaction.get("Billing Address", "unknown"),
                int(transaction.get("Account Age Days", 0)),
                int(transaction.get("Transaction Hour", 0)),
                int(transaction.get("Is Fraudulent", 0))
            ))
        except Exception as e:
            logger.error(f"Erreur lors du traitement de la transaction {transaction_id} : {str(e)}")
            continue
    try:
        session.execute(batch)
        logger.info(f"Lot de {len(transactions)} transactions inséré dans Cassandra")
    except Exception as e:
        logger.error(f"Échec de l'insertion du lot de transactions : {str(e)}")

def process_fraud_batch(session, fraud_results, fraud_insert):
    """Processes a batch of fraud results and inserts into Cassandra."""
    batch = BatchStatement()
    for result in fraud_results:
        transaction_id = result.get("transaction_id", "unknown")
        try:
            # Convert timestamp to datetime
            timestamp = datetime.strptime(
                result.get("timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "%Y-%m-%d %H:%M:%S"
            )
            batch.add(fraud_insert, (
                transaction_id,
                bool(result.get("is_fraud", False)),
                timestamp,
                float(result.get("amount", 0.0)),
                result.get("payment_method", "unknown"),
                result.get("product_category", "unknown"),
                result.get("error")
            ))
        except Exception as e:
            logger.error(f"Erreur lors du traitement du résultat de fraude {transaction_id} : {str(e)}")
            continue
    try:
        session.execute(batch)
        logger.info(f"Lot de {len(fraud_results)} résultats de fraude inséré dans Cassandra")
    except Exception as e:
        logger.error(f"Échec de l'insertion du lot de résultats de fraude : {str(e)}")

def main():
    """Main function to consume from both topics and insert into Cassandra."""
    session = None
    try:
        # Initialize Cassandra
        session = connect_to_cassandra()
        transaction_insert, fraud_insert = prepare_statements(session)

        # Initialize Kafka consumer for both topics
        logger.info("Initialisation du consommateur Kafka...")
        consumer = KafkaConsumer(
            INPUT_TOPIC, OUTPUT_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset=AUTO_OFFSET_RESET,
            group_id='cassandra_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            session_timeout_ms=60000,
            heartbeat_interval_ms=20000,
            max_poll_interval_ms=600000
        )
        logger.info(f"Consommateur Kafka démarré pour les topics {INPUT_TOPIC} et {OUTPUT_TOPIC}")

        transaction_batch = []
        fraud_batch = []
        for message in consumer:
            try:
                topic = message.topic
                data = message.value
                if topic == INPUT_TOPIC:
                    transaction_id = data.get("Transaction ID", "unknown")
                    logger.info(f"Transaction reçue de {INPUT_TOPIC} : {transaction_id}")
                    transaction_batch.append(data)
                    if len(transaction_batch) >= BATCH_SIZE:
                        process_transaction_batch(session, transaction_batch, transaction_insert)
                        transaction_batch = []
                elif topic == OUTPUT_TOPIC:
                    transaction_id = data.get("transaction_id", "unknown")
                    logger.info(f"Résultat de fraude reçu de {OUTPUT_TOPIC} : {transaction_id}")
                    fraud_batch.append(data)
                    if len(fraud_batch) >= BATCH_SIZE:
                        process_fraud_batch(session, fraud_batch, fraud_insert)
                        fraud_batch = []
            except Exception as e:
                logger.error(f"Erreur lors du traitement du message du topic {topic} : {str(e)}")
                continue

        # Process remaining batches
        if transaction_batch:
            process_transaction_batch(session, transaction_batch, transaction_insert)
        if fraud_batch:
            process_fraud_batch(session, fraud_batch, fraud_insert)

    except Exception as e:
        logger.error(f"Erreur fatale dans la boucle principale : {str(e)}")
    finally:
        if session:
            session.cluster.shutdown()
            logger.info("Connexion Cassandra fermée")

if __name__ == "__main__":
    main()