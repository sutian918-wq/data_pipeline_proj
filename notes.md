# What is this project about?
>>> This project is simulating a data pipeline in real life where a producer uses a thread pool to fetch data from a source and produce(send) these messages to Kafka broker. The ticker in the data acts as the key that determines the partition the data would go to. A consumer is initialised and would be running a poll loop. It continuosly polls data from Kafka and send the data to worker queues according to their partition so the order in each partition is maintained. Each worker queue is managed by a thread which sends the data to a processor and writes it to database.  

# Problem faced
>>> I had both the main poll loop and the worker threads calling consumer.commit() on the same Consumer object. This silently causes the Consumer's internal state to be corrupted occasionally without showing any obvious errors (showed like "STALE_MEMBER_EPOCH" and is not reproducible). This is because the Kafka consumer is not thread-safe so concurrent commits led to a race-condition when two threads called the same Consumer instance. This problem is only realised under load. 

>>> Consumer too slow ~20 msgs consumed, processed and inserted per sec. Reason: used consumer.poll() in the consumer loop and insert each msg into the worker queue one by one - slow

# The Fix
>>> Use a "commit queue" so workers now push (partition, offset) tuples to a thread-safe queue after successfully writing to the database. Only the main poll loop drains that queue and calls commit(). This serialises all consumer operations to one thread, and it also batches commits - so it's faster and safer.

>>> Use consumer.consume(N messages, timeout=1.0) instead to get N messages a time as well as batch inserts to postgres. Increased the consumer throughput to ~60,000 msgs/sec. 

# Benchmark
>>> # Kafka cluster's raw performance - Baseline
>>> # Producer (kafka-producer-perf-test.sh)
>>>>>> 100000 records sent, 240384.615385 records/sec (114.62 MB/sec), 48.71 ms avg latency, 116.00 ms max latency, 49 ms 50th, 74 ms 95th, 75 ms 99th, 75 ms 99.9th.

>>> # Python Kafka-pipeline
>>> # Producer
>>>>>> 221K/sec



# Postgre commands
>>> import psycopg2

# Connect to an existing database
>>> conn = psycopg2.connect("dbname=test user=postgres")

# Open a cursor to perform database operations
>>> cur = conn.cursor()

# Execute a command: this creates a new table
>>> cur.execute("CREATE TABLE test (id serial PRIMARY KEY, num integer, data varchar);")

# Pass data to fill a query placeholders and let Psycopg perform
# the correct conversion (no more SQL injections!)
>>> cur.execute("INSERT INTO test (num, data) VALUES (%s, %s)",
...      (100, "abc'def"))

# Query the database and obtain data as Python objects
>>> cur.execute("SELECT * FROM test;")
>>> cur.fetchone()
(1, 100, "abc'def")

# Make the changes to the database persistent
>>> conn.commit()

# Close communication with the database
>>> cur.close()
>>> conn.close()


