# market_producer.py
from kafka import KafkaProducer
import json
import time
import requests

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# API Configuration
API_KEY = 'WVLO7HP2SY5I4L7U'  # Alpha Vantage API Key
BASE_URL = 'https://www.alphavantage.co/query?function='


def fetch_market_data():
    params = f"TIME_SERIES_INTRADAY&apikey={API_KEY}&symbol=IBM&interval=1min"
    response = requests.get(f"{BASE_URL}{params}",)
    return response.json()

if __name__ == "__main__":

    while True:
        market_data = fetch_market_data()
        producer.send('market-data', market_data)
        print(f"Sent market data: {market_data}")
        time.sleep(60)  # Fetch data every 60 seconds