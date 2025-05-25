# api/main.py
from fastapi import FastAPI, BackgroundTasks
from kafka import KafkaProducer
import pandas as pd
import time
import json
import os
import logging

# Configuration
CSV_PATH = os.getenv("CSV_PATH", "/app/data/Fraudulent_E-Commerce_Transaction_Data.csv")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TRANSACTION_TOPIC = "transaction_topic"

# Logging
logging.basicConfig(level=logging.INFO)

app = FastAPI()

# Kafka Producer avec configuration optimisée
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    acks='all',
    retries=5,
    retry_backoff_ms=1000,
    batch_size=16384,
    linger_ms=500
)

@app.on_event("shutdown")
def shutdown_event():
    """Ferme le producteur Kafka proprement."""
    logging.info("Fermeture du producteur Kafka...")
    producer.close()

@app.get("/")
def root():
    return {"message": "API d'ingestion CSV vers Kafka"}

def send_csv_to_kafka():
    """Tâche en arrière-plan pour envoyer les données CSV vers Kafka."""
    try:
        df = pd.read_csv(CSV_PATH, encoding='latin1')
        logging.info(f"Chargement du CSV réussi. {len(df)} lignes détectées.")
        
        for _, row in df.iterrows():
            producer.send(TRANSACTION_TOPIC, row.to_dict())
            time.sleep(0.5)  # Simule un débit contrôlé

        logging.info("Envoi des données vers Kafka terminé.")
    except FileNotFoundError:
        logging.error(f"Fichier CSV introuvable : {CSV_PATH}")
    except Exception as e:
        logging.exception(f"Erreur lors de l'ingestion CSV : {e}")

@app.post("/ingest_csv/")
def ingest_csv(background_tasks: BackgroundTasks):
    background_tasks.add_task(send_csv_to_kafka)
    return {"message": "L'ingestion CSV a démarré en arrière-plan"}