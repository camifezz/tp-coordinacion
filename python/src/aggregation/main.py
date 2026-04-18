import os
import logging
import bisect

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.fruit_top = {}  # {client_id: [FruitItem]}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data for client {client_id}")
        client_top = self.fruit_top.setdefault(client_id, [])
        for i in range(len(client_top)):
            if client_top[i].fruit == fruit:
                client_top[i] = client_top[i] + fruit_item.FruitItem(fruit, amount)
                return
        bisect.insort(client_top, fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info(f"Received EOF for client {client_id}")
        client_top = self.fruit_top.pop(client_id, [])
        fruit_chunk = list(client_top[-TOP_SIZE:])
        fruit_chunk.reverse()
        top = [(fi.fruit, fi.amount) for fi in fruit_chunk]
        self.output_queue.send(message_protocol.internal.serialize([client_id, top]))

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._process_eof(*fields)
        ack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def close(self):
        self.input_exchange.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    aggregation_filter = AggregationFilter()
    try:
        aggregation_filter.start()
    except middleware.MessageMiddlewareDisconnectedError:
        logging.error("Lost connection to the message broker")
        return 1
    except middleware.MessageMiddlewareMessageError:
        logging.error("Internal middleware error")
        return 1
    finally:
        aggregation_filter.close()
    return 0


if __name__ == "__main__":
    main()
