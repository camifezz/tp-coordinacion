import os
import logging
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
        self.fruit_data = {}  # {client_id: {fruit: FruitItem}}
        self.eof_count = {}   # {client_id: int}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data for client {client_id}")
        client_data = self.fruit_data.setdefault(client_id, {})
        client_data[fruit] = client_data.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, amount)

    def _process_eof(self, client_id):
        count = self.eof_count.get(client_id, 0) + 1
        self.eof_count[client_id] = count
        logging.info(f"Received EOF {count}/{SUM_AMOUNT} for client {client_id}")

        if count < SUM_AMOUNT:
            return

        del self.eof_count[client_id]
        client_data = self.fruit_data.pop(client_id, {})
        sorted_fruits = sorted(client_data.values())
        top = [(fi.fruit, fi.amount) for fi in sorted_fruits[-TOP_SIZE:][::-1]]
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
        try:
            aggregation_filter.close()
        except middleware.MessageMiddlewareCloseError:
            logging.error("Error closing middleware connections")
    return 0


if __name__ == "__main__":
    main()
