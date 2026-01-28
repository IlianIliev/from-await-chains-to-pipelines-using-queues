"""
Using Sized Buffers

Here we overcome the problem described in the previous implementation,
by setting a max size of the queue. Once the max size is reached, the
put operation is blocked until an item has been taken out of the queue.
"""

import asyncio
import dataclasses
import time

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
    print("Storing batch:{} after {} seconds.".format(batch.id, SLEEP))


async def producer_loop(producer: BatchProducer, producer_buffer: asyncio.Queue):
    while True:
        batch = await producer.get_batch()
        await producer_buffer.put(batch)


async def transformer_loop(
    producer_buffer: asyncio.Queue, transformer_buffer: asyncio.Queue
):
    while True:
        batch = await producer_buffer.get()
        batch = await transformer(batch)
        await transformer_buffer.put(batch)
        producer_buffer.task_done()


async def store_loop(transformer_buffer):
    while True:
        transformed_batch = await transformer_buffer.get()
        await store(transformed_batch)
        transformer_buffer.task_done()


async def main():
    producer_buffer = asyncio.Queue(maxsize=5)
    transformer_buffer = asyncio.Queue(maxsize=5)

    # Create tasks for each loop
    producer_task = asyncio.create_task(producer_loop(BatchProducer(), producer_buffer))
    transformer_task = asyncio.create_task(
        transformer_loop(producer_buffer, transformer_buffer)
    )
    store_task = asyncio.create_task(store_loop(transformer_buffer))

    # Wait for all tasks to complete (they won't in this case as they're infinite loops)
    await asyncio.gather(producer_task, transformer_task, store_task)


if __name__ == "__main__":
    asyncio.run(main())
