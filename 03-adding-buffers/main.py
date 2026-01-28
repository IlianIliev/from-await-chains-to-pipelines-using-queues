"""
Adding the Buffers

Here, we are finally doing async properly.
Each component from out pipeline - the producer, transformer and store,
run in its own loop. The results from the operations are stored in queues
and this way shared between the different components.

The downside of this implementation is that due to the lack of sizing of
the queue, it is possible that one of processes may overtake from the other
one. In the scenario of the producer being faster than the rest, this may
result in the queue growing beyond the system limits.
"""

import asyncio
import dataclasses
import time

SLEEP = 0.5
PRODUCER_SLEEP = 0.1


@dataclasses.dataclass
class Batch:
    id: int


class BatchProducer:
    next_batch_id: int = 0

    async def get_batch(self) -> Batch:
        batch = Batch(self.next_batch_id)
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
        transformed_batch = await transformer(batch)
        await transformer_buffer.put(transformed_batch)
        producer_buffer.task_done()


async def store_loop(transformer_buffer):
    while True:
        start = time.time()
        transformed_batch = await transformer_buffer.get()
        await store(transformed_batch)
        transformer_buffer.task_done()
        print("---Batch took {:.2f} seconds.---".format(time.time() - start))


async def main():
    producer_buffer = asyncio.Queue()
    transformer_buffer = asyncio.Queue()

    # Create tasks for each loop
    # Note that here we use dependency injection to pass the buffer to the component that
    # needs to access it.
    producer_task = asyncio.create_task(producer_loop(BatchProducer(), producer_buffer))
    transformer_task = asyncio.create_task(
        transformer_loop(producer_buffer, transformer_buffer)
    )
    store_task = asyncio.create_task(store_loop(transformer_buffer))

    # Wait for all tasks to complete (they won't in this case as they're infinite loops)
    # In this case instead of calling the tasks directly, we run the tasks (the awaitable
    # objects) concurrently, allowing them to switch context from one to another when
    # blocked
    await asyncio.gather(producer_task, transformer_task, store_task)


if __name__ == "__main__":
    asyncio.run(main())
