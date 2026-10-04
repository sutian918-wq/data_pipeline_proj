# What is this project about?
>>> This project is simulating a data pipeline in real life where a producer uses a pool of threads to fetch data from a source and produce(send) these messages to Kafka broker. The ticker in the data acts as the key that determines the partition the data would go to. A consumer is initialised and would be running a poll loop. It continuosly polls data from Kafka and send the data to worker queues according to their hashed key(ticker) so the order in each partition is maintained. Each worker queue is managed by a thread which send the data to a processor and writes it to database.  

# Problem
>>> I had the same Consumer instance committing in both the main consumer loop as well as the worker thread loop. This silently causes the Consumer's internal state to be corrupted without showing any obvious errors (showed like "STALE_MEMBER_EPOCH" and is not reproducible). This is because commit() calls are not thread-safe so it led to a race-condition when two threads called the same Consumer instance. I only realise this problem under load. 

# Fix
>>> Use a commit queue so worker threads only put processed data into this queue instead of committing it. Only the main consumer loop manage commits.



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


