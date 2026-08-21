from confluent_kafka import Consumer, KafkaError, KafkaException
import os
import time
from dotenv import load_dotenv 

load_dotenv()

def commit_completed(err, partitions):
    if err:
        print(f"Commit failed: {err}")
    else:
        print(f"Committed: {partitions}")

conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processors',
    'auto.offset.reset': 'earliest', # start reading from the earliest message if no offset is stored for this group
    'enable.auto.commit': False, # commit manually after processing to avoid data loss
    'on_commit': commit_completed # spot silent commit failures
}

consumer = Consumer(conf)

TOPIC_NAME = os.getenv("TOPIC_NAME", 'market_data')
consumer.subscribe([TOPIC_NAME])

running = True
msg_count = 0
last_commit_time = time.time()
COMMIT_INTERVAL_SECONDS = 5
BATCH_SIZE = 100

# process message
def process_tick(msg):
    print(f'Received message: {msg.value().decode("utf-8")}')

try: 
    while running:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                # reached end of partition, continue
                continue
            else:
                raise KafkaException(msg.error())

        # process the message 
        process_tick(msg)
        msg_count += 1

        # commit after each batch or time interval
        current_time = time.time()
        if msg_count >= BATCH_SIZE or current_time - last_commit_time >= COMMIT_INTERVAL_SECONDS:
            consumer.commit()
            msg_count = 0
            last_commit_time = current_time

except KeyboardInterrupt:
    print("Shutting down...")
finally:
    # Commit any remaining messages and clean up
    print("Final commit...")
    consumer.commit(asynchronous=False)
    consumer.close()
    print("Consumer closed.")

