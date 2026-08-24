import json
import time 
import random
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor, as_completed

# Load environment variables from .env
load_dotenv()

# constants
TOPIC = os.getenv("TOPIC_NAME", 'market_data')
MAX_WORKERS = int(os.getenv("PRODUCER_THREADS",4))

# log every delivery for debug
def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message {msg.value().decode("utf-8")} delivered to {msg.topic()} [{msg.partition()}]')

# Producer configuration
conf = {
    'bootstrap.servers': 'localhost:9092', 
    'queue.buffering.max.kbytes': 32768, # 32 MB max buffer size
    'batch.size': 16384, # 16 KB max per batch, larger means fewer network requests
    'linger.ms': 10, # the max time producer will wait before sending a batch of messages
    'compression.type': 'lz4', # save bandwidth and improve throughput
    'on_delivery': delivery_report,
}

producer = Producer(conf)

tickers = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX', 'INTC', 'AMD']

# sending logic
def send_tick(ticker):
    # simulate stock data
    data = {
        "ticker": ticker,
        "price": round(random.uniform(100,500),2),
        "timestamp": time.time()
    }
    # Send to Kafka topic 'market_data'
    producer.produce(TOPIC, json.dumps(data))


# spin up 4 threads, one for each ticker
with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
    # submit all tasks
    future_to_ticker = { executor.submit(send_tick, t): t for t in tickers }

    # process each completed future as it finishes
    for future in as_completed(future_to_ticker):
        ticker = future_to_ticker[future]
        try:
            future.result()
        except Exception as e:
            print(f"Tick failed: {e} for ticker {ticker}")

# flush all messages to Kafka
producer.flush()
    