import dataclasses
import time

SLEEP = 0.3


@dataclasses.dataclass
class Batch:
    id: int


class BatchProducer:
    next_batch_id: int = 0

    def get_batch(self) -> Batch:
        batch = Batch(self.next_batch_id)
        time.sleep(SLEEP)
        print("Producing Batch:{} after {} seconds.".format(batch.id, SLEEP))
        self.next_batch_id += 1

        return batch


def transformer(batch: Batch) -> Batch:
    time.sleep(SLEEP)
    print("Transforming Batch:{} after {} seconds.".format(batch.id, SLEEP))

    return batch


def store(batch: Batch):
    time.sleep(SLEEP)
    print("Storing batch:{} after {} seconds.".format(batch.id, SLEEP))


def main():
    producer = BatchProducer()

    while True:
        start = time.time()

        batch = producer.get_batch()
        transformed_batch = transformer(batch)
        store(transformed_batch)

        print(
            "---Batch:{} took {:.2f} seconds.---".format(batch.id, time.time() - start)
        )


if __name__ == "__main__":
    main()
