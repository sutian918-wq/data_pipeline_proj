"""
Partition-keyed Kafka consumer.

Architecture:
- One poll loop (main thread) dispatches messages to worker queues.
- N worker threads, each with its own queue.
- Messages are routed to workers by 'partition % N', preserving
  per-partition ordering while allowing parallel processing.
"""

import json
import logging
import threading
from queue import Empty, Queue as queue
from src.config import settings
import time

from confluent_kafka import Consumer, KafkaError, TopicPartition
from src.consumers import BaseConsumer

logger = logging.getLogger(__name__)

class KafkaConsumer(BaseConsumer):
    """
    Partition-keyed Kafka consumer.

    Routing rule: worker_id = partition % num_workers.
    This guarantees:
    - All messages for a partition go to the same worker.
    - That worker processes them in order.
    - Different partitions are processed in parallel.
    """

    def __init__(self, processor, writer, config: dict | None = None, num_workers: int = 5, queue_size: int = 1000, max_poll_records: int = 100):

        super().__init__(processor, writer, config)

        self.num_workers = num_workers
        self.queue_size = queue_size
        self.topic = settings.topic_name

        kafka_config = {
            'bootstrap.servers': settings.kafka_bootstrap_servers, 
            'group.id': settings.consumer_group, # comsumer's group id
            'auto.offset.reset': 'earliest', # start reading from the earliest message if no offset is stored for this group
            'enable.auto.commit': False, # commit manually after processing to avoid data loss
            'max.poll.records': max_poll_records, # maximum number of messages to fetch in a single poll
        }

        final_config = {**kafka_config, **(config or {})}
        self.consumer = Consumer(final_config)

        self.consumer.subscribe([self.topic])

        self.insert_batch_size = settings.postgres_batch_size

        self.worker_queues: list[queue] = [
            queue(maxsize=queue_size) for _ in range(num_workers)
        ]
        self.worker_threads: list[threading.Thread] = []

        self.commit_queue = queue()

        logger.info(
            f"KafkaConsumer initialised: {num_workers} workers, "
            f"queue size {queue_size}, topic '{self.topic}'"       
        )

    def commit_callback(self, err, partitions):
        """Log commit results for debug."""
        if err:
            logger.error(f"Commit failed: {err}")
        else:
            for p in partitions:
                logger.info(f"Successfully committed: partition '{p}', offset {p.offset}")

    def _start_workers(self) -> None:
        """Launch all worker threads."""
        for worker_id in range(self.num_workers):
            t = threading.Thread(
                target=self._worker_loop,
                args=(worker_id,),
                name=f"worker-{worker_id}",
                daemon=True
            )
            t.start()
            self.worker_threads.append(t)
        logger.info(f"Started {self.num_workers} worker threads")

    def _stop_workers(self) -> None:
        """Signal each worker thread to exit and wait for them."""
        # Push indicator to wake workers
        for q in self.worker_queues:
            q.put(None)
        for t in self.worker_threads:
            t.join(timeout=10)
            if t.is_alive():
                logger.warning(f"Worker {t.name} did not exit cleanly")
        logger.info("All workers stopped")

    def _worker_loop(self, worker_id: int) -> None:
        q = self.worker_queues[worker_id]
        logger.info(f"Worker {worker_id} started")
        while True:
            batch = []

            stop_received = False

            msg = q.get()
            if msg is None:
                logger.info(f"Worker {worker_id} received stop indicator, exiting")
                break

            batch.append(msg)

            while len(batch) < self.insert_batch_size:
                try:
                    msg = q.get(timeout=0.05)
                except Empty:
                    break

                if msg is None:
                    stop_received = True
                    break

                batch.append(msg)

            try:
                self._process_messages(batch)
            except Exception as e:
                logger.error(
                    f"Worker {worker_id} error processing batch of "
                    f"{len(batch)}: {e}",
                    exc_info=True,
                )
            finally:
                for _ in batch:
                    q.task_done() # tell queue consumer that this item is processed 

            if stop_received:
                logger.info(f"Worker {worker_id} received stop indicator, exiting")
                break

    # def _process_message(self, msg) -> None:
    #     try:
    #         data = json.loads(msg.value().decode("utf-8"))

    #     except (json.JSONDecodeError, UnicodeDecodeError) as e:
    #         logger.warning(
    #         f"Skipping non-JSON message at p={msg.partition()} o={msg.offset()}: {e}"
    #         )
    #         # Commit anyway so don't loop forever
    #         self.commit_queue.put((msg.partition(), msg.offset()))
    #         return
        
    #     data['partition_id'] = msg.partition()
    #     data['offset_id'] = msg.offset()

    #     processed = self.processor.process(data)
    #     if not processed:
    #         logger.warning(
    #             f"Processor rejected message "
    #             f"(partition: {msg.partition()}, offset: {msg.offset()})"
    #         )
    #         return 

    #     self.writer.insert(processed)

    #     # Put in commit queue after successful write
    #     self.commit_queue.put((msg.partition(), msg.offset()))

    def _process_messages(self, msgs) -> None:
        t0 = time.time()
        processed = []
        last_offsets = {}
        for msg in msgs:
            try:
                data = json.loads(msg.value().decode("utf-8"))
            
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.warning(
                    f"Skipping non-JSON message at p={msg.partition()} o={msg.offset()}: {e}"
                )
                # Commit anyway so don't loop forever
                last_offsets[msg.partition()] = msg.offset()
                continue
            data['partition_id'] = msg.partition()
            data['offset_id'] = msg.offset()
            data = self.processor.process(data)
            if not data:
                logger.warning(
                    f"Processor rejected message "
                    f"(partition: {msg.partition()}, offset: {msg.offset()})"
                )
                last_offsets[msg.partition()] = msg.offset()
                continue
            
            processed.append(data)
            last_offsets[msg.partition()] = msg.offset()

        if not processed:
            return 

        t1 = time.time()
        self.writer.insert_batch(processed)
        t2 = time.time()
        logger.info(
            f"Batch size={len(processed)} parse={t1-t0:.3f}s "
            f"insert={t2-t1:.3f}s"
        )
        for partition, offset in last_offsets.items():
            self.commit_queue.put((partition, offset))
    
    def _consume_loop(self):
        """
            While true, poll from kafka and put into queue. process each queue with a thread.
        """
        self._start_workers()
        logger.info("Starting poll loop")
        try:
            while not self.stop_event.is_set():
                self._drain_commit_queue()

                msgs = self.consumer.consume(
                    num_messages=self.insert_batch_size * self.num_workers,
                    timeout=1.0,
                )

                for msg in msgs:
                    if msg is None:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        logger.error(f"Kafka error: {msg.error()}")
                        continue

                    # Route to worker by partition
                    partition = msg.partition()
                    worker_id = partition % self.num_workers
                    try:
                        # Blocks if queue is full -> backpressure
                        self.worker_queues[worker_id].put(msg)
                    except Exception as e:
                        logger.error(f"Failed to enqueue message: {e}")
                
        finally:
            logger.info("Poll loop exited")
            self._stop_workers()
            self._drain_commit_queue()

    def _drain_commit_queue(self) -> None:
        offsets = []

        while True:
            try:
                partition, offset = self.commit_queue.get_nowait()
                offsets.append(TopicPartition(self.topic, partition=partition, offset=offset+1))
            except Empty:
                break

        if offsets:
            try:
                self.consumer.commit(offsets=offsets, asynchronous=False)
                logger.info(f"Committed {len(offsets)} offsets.")
            except Exception as e:
                logger.error(f"Batch commit failed: {e}")


    def _cleanup(self) -> None:
        logger.info("Cleaning up...")

        try:
            self.consumer.close()
        except Exception as e:
            logger.error(f"Error closing consumer: {e}")

        try:
            self.writer.close()
        except Exception as e:
            logger.error(f"Error closing writer: {e}")

        logger.info("Cleanup complete")







        
