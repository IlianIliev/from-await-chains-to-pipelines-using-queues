"""
Going Async

Here we have changed the methods to be asynchronous, and use `asyncio.sleep`
to stimulate the network delays, but the performance is still bad.
The reason is that here we have an "await-chain" the asynchronous processes
are ready to switch context, but due to the chaining there is nothing else
to pick work - the next method in the chain is actually waiting for the
previous to complete execution and return result.
"""

import asyncio
import dataclasses
import time

SLEEP = 0.3


@dataclasses.dataclass
class Batch:
    id: int


class BatchProducer:
    next_batch_id: int = 0

    async def get_batch(self) -> Batch:
        batch = Batch(self.next_batch_id)
        await asyncio.sleep(SLEEP)
        print("Producing Batch:{} after {} seconds.".format(batch.id, SLEEP))
        self.next_batch_id += 1

        return batch


async def transformer(batch: Batch) -> Batch:
    await asyncio.sleep(SLEEP)
    print("Transforming Batch:{} after {} seconds.".format(batch.id, SLEEP))

    return batch


async def store(batch: Batch):
    await asyncio.sleep(SLEEP)
    print("Storing batch:{} after {} seconds.".format(batch.id, SLEEP))


async def main():
    producer = BatchProducer()

    while True:
        start = time.time()

        batch = await producer.get_batch()
        transformed_batch = await transformer(batch)
        await store(transformed_batch)

        print("---Batch took {:.2f} seconds.---".format(time.time() - start))


if __name__ == "__main__":
    asyncio.run(main())
