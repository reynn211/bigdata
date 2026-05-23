import csv
import json
import os
import time
import glob
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
TOPIC = os.environ.get("KAFKA_TOPIC", "mock_data")
DATA_DIR = os.environ.get("DATA_DIR", "/data")


def wait_for_kafka(broker, retries=30, delay=5):
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[producer] Connected to Kafka at {broker}")
            return producer
        except (KafkaError, OSError) as e:
            print(f"[producer] Kafka not ready (attempt {attempt}/{retries}): {e.__class__.__name__}; retrying in {delay}s...")
            time.sleep(delay)
    raise RuntimeError(f"Could not connect to Kafka at {broker} after {retries} attempts")


def read_csv_files(data_dir):
    pattern = os.path.join(data_dir, "*.csv")
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No CSV files found in {data_dir}")
    print(f"[producer] Found {len(files)} CSV file(s) in {data_dir}")
    return files


def send_rows(producer, files, topic):
    total = 0
    for filepath in files:
        filename = os.path.basename(filepath)
        count = 0
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                row["global_sale_id"] = str(total)
                producer.send(topic, value=row)
                count += 1
        print(f"[producer] Sent {count} rows from {filename}")
    producer.flush()
    print(f"[producer] Total rows sent: {total}")


def main():
    print("[producer] Starting Kafka producer...")
    producer = wait_for_kafka(KAFKA_BROKER)
    files = read_csv_files(DATA_DIR)
    send_rows(producer, files, TOPIC)
    print("[producer] Done. All rows published to topic:", TOPIC)
    producer.close()


if __name__ == "__main__":
    main()
