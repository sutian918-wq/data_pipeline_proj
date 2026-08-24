import os
import time
from dotenv import load_dotenv 
from confluent_kafka import Consumer, KafkaError, KafkaException

# load environment variables from .env
load_dotenv()

# constants
TOPIC_NAME = os.getenv("TOPIC_NAME", 'market_data')
BATCH_SIZE = 1
COMMIT_INTERVAL_SECONDS = 5

# log commit results for debug
def commit_completed(err, partitions):
    if err:
        print(f"Commit failed: {err}")
    else:
        print(f"Committed: {partitions}")

# consumer configuration
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'processors',
    'auto.offset.reset': 'earliest', # start reading from the earliest message if no offset is stored for this group
    'enable.auto.commit': False, # commit manually after processing to avoid data loss
    'on_commit': commit_completed # spot silent commit failures
}

consumer = Consumer(conf)

consumer.subscribe([TOPIC_NAME])

# process message
def process_tick(msg):
    print(f'Received message: {msg.value().decode("utf-8")}')

running = True
msg_count = 0
last_commit_time = time.time()
try: 
    while running:
        msg = consumer.poll(timeout=1.0)

        if msg is None: 
            # no message received, continue polling
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
    
    if msg_count > 0:
        try:
            consumer.commit(asynchronous=False)
            print("Final commit successful")
        except Exception as e:
            print(f"Final commit failed: {e}")
    else: 
        print("No messages to commit.")

    consumer.close()
    print("Consumer closed.")

