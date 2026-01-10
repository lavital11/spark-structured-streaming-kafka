import json
import time
import pandas as pd
from kafka import KafkaProducer

# ====== CONFIG ======
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "electric_vehicles"
CSV_PATH = "../input/electric_vehicle_population.csv"

# ====== PRODUCER ======
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# ====== READ CSV ======
df = pd.read_csv(CSV_PATH)

print(f"Loaded {len(df)} rows from CSV")

# ====== SEND TO KAFKA ======
for _, row in df.iterrows():
    message = row.to_dict()
    producer.send(TOPIC_NAME, value=message)
    time.sleep(0.01)

producer.flush()
producer.close()

print("All messages sent to Kafka ✔")
