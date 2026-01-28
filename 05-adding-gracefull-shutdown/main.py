"""
Adding Graceful Shutdown

To ensure a graceful termination, meaning that after receiving a shutdown signal
the pipeline stops producing items and drains out the items from the queues before
exiting.

The first component of the pipeline depends on the `shutdown_event` event to decide
when to stop producing tasks, while the latter components check if the previous
task is completed and if the queue is empty to decide whether they are done or not
"""

import asyncio
import dataclasses
import time
import signal

SLEEP = 0.5
PRODUCER_SLEEP = 0.1


@dataclasses.dataclass
class Batch:
    id: int
    created_at: float


class BatchProducer:
    next_batch_id: int = 0

    async def get_batch(self) -> Batch:
        batch = Batch(self.next_batch_id, created_at=time.perf_counter())
        await asyncio.sleep(PRODUCER_SLEEP)
        print("Producing Batch:{} after {} seconds.".format(batch.id, PRODUCER_SLEEP))
        self.next_batch_id += 1

        return batch


async def transformer(batch: Batch) -> Batch:
    await asyncio.sleep(SLEEP)
    print("Transforming Batch:{} after {} seconds.".format(batch.id, SLEEP))

    return batch


async def store(batch: Batch):
    await asyncio.sleep(SLEEP)
    print(
        "Storing batch:{} after {} seconds. Total batch time: {:.2f}".format(
            batch.id, SLEEP, time.perf_counter() - batch.created_at
        )
    )


async def producer_loop(
    producer: BatchProducer,
    producer_buffer: asyncio.Queue,
    shutdown_event: asyncio.Event,
):
    while True:
        if shutdown_event.is_set():
            break

        batch = await producer.get_batch()
        await producer_buffer.put(batch)


async def transformer_loop(
    producer_buffer: asyncio.Queue,
    transformer_buffer: asyncio.Queue,
    producer_task: asyncio.Task,
):
    while True:
        if producer_task.done() and producer_buffer.empty():
            break

        batch = await producer_buffer.get()
        transformed_batch = await transformer(batch)
        await transformer_buffer.put(transformed_batch)

        # While not needed in this example, it is a good practice
        # to call task_done() after the processing of the item from
        # the queue is done.
        # This is required if your code uses queue.join() but in this
        # case we use other mechanism to finish execution
        producer_buffer.task_done()


async def store_loop(transformer_buffer: asyncio.Queue, transformer_task: asyncio.Task):
    while True:
        if transformer_task.done() and transformer_buffer.empty():
            break

        transformed_batch = await transformer_buffer.get()
        await store(transformed_batch)
        transformer_buffer.task_done()


async def main():
    producer_buffer = asyncio.Queue(maxsize=5)
    transformer_buffer = asyncio.Queue(maxsize=5)

    shutdown_event = asyncio.Event()

    # Set up signal handler for keyboard interrupt
    # This will allow us to intercept the interruption signal and start
    # the process of graceful termination.
    def signal_handler():
        if not shutdown_event.is_set():
            print("\nKeyboard interrupt detected. Starting graceful shutdown...")
            print(f"Producer buffer size: {producer_buffer.qsize()}")
            print(f"Transformer buffer size: {transformer_buffer.qsize()}")
            shutdown_event.set()
        else:
            print("\nShutdown already in progress. Please wait for buffers to empty...")

    # Register the signal handler with the event loop
    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, signal_handler)

    # Create tasks for each loop
    producer_task = asyncio.create_task(
        producer_loop(BatchProducer(), producer_buffer, shutdown_event)
    )
    transformer_task = asyncio.create_task(
        transformer_loop(producer_buffer, transformer_buffer, producer_task)
    )
    store_task = asyncio.create_task(store_loop(transformer_buffer, transformer_task))

    start_time = time.perf_counter()
    await asyncio.gather(producer_task, transformer_task, store_task)
    end_time = time.perf_counter()

    print("Total processing time: {:.2f} seconds.".format(end_time - start_time))


if __name__ == "__main__":
    asyncio.run(main())
