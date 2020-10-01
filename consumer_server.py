import asyncio

from confluent_kafka import Consumer, Producer, OFFSET_BEGINNING
from confluent_kafka.admin import AdminClient, NewTopic


BROKER_URL = "localhost:9092"
TOPIC_NAME = 'police.dep.service'


async def consume(topic_name):
    """Consumes data from the Kafka Topic"""
    # Sleep for a few seconds to give the producer time to create some data
    await asyncio.sleep(2.5)

    # TODO: Set the offset reset to earliest
    c = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            #"group.id": "0",
            "auto.offset.reset": "earliest",
        }
    )

    # TODO: Configure the on_assign callback
    c.subscribe([topic_name])

    while True:
        message = c.poll(1.0)
        if message is None:
            print("no message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            print(f"consumed message {message.key()}: {message.value()}")
        await asyncio.sleep(0.1)



def consumer():
    """Runs the exercise"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    try:
        asyncio.run(consume(TOPIC_NAME))
    except KeyboardInterrupt as e:
        print("shutting down")



if __name__ == "__main__":
    main()
