import json
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

KAFKA_TOPIC = 'input'
KAFKA_SERVER = 'kafka:9092'
WIKIMEDIA_STREAM_URL = 'https://stream.wikimedia.org/v2/stream/page-create'

def create_producer():
    """Створює Kafka продюсера з кількома спробами підключення."""
    for i in range(5):
        try:
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_SERVER],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Successfully connected to Kafka!")
            return producer
        except KafkaError as e:
            print(f"Failed to connect to Kafka: {e}. Retrying in 10 seconds...")
            time.sleep(10)
    raise ConnectionError("Could not connect to Kafka after several retries.")

def main():
    producer = create_producer()
    print(f"Connecting to Wikimedia stream: {WIKIMEDIA_STREAM_URL}")
    
    try:
        with requests.get(WIKIMEDIA_STREAM_URL, stream=True) as response:
            response.raise_for_status()
            for line in response.iter_lines():
                if line.startswith(b'data:'):
                    try:
                        data_str = line.decode('utf-8').replace('data: ', '')
                        data_json = json.loads(data_str)
                        producer.send(KAFKA_TOPIC, value=data_json)
                        print(f"Sent: {data_json.get('page_title', 'N/A')}")
                    except (json.JSONDecodeError, UnicodeDecodeError) as e:
                        print(f"Error decoding line: {e}")
    except requests.exceptions.RequestException as e:
        print(f"Error connecting to stream: {e}")
    finally:
        producer.flush()
        producer.close()
        print("Producer closed.")

if __name__ == "__main__":
    main()
