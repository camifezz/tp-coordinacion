import os
import logging

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.eof_output_queues = []
        for i in range(SUM_AMOUNT):
            eof_output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
                MOM_HOST, f"{SUM_PREFIX}_{i}_eof"
            )
            self.eof_output_queues.append(eof_output_queue)
        self.amount_by_fruit = {}  # {client_id: {fruit: FruitItem}}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Processing data for client {client_id}")
        client_fruits = self.amount_by_fruit.setdefault(client_id, {})
        client_fruits[fruit] = client_fruits.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _broadcast_eof(self, client_id):
        logging.info(f"Broadcasting EOF for client {client_id} to all sum workers")
        for eof_queue in self.eof_output_queues:
            eof_queue.send(message_protocol.internal.serialize([client_id]))

    def _process_eof(self, client_id):
        logging.info(f"Sending totals for client {client_id} to aggregation")
        client_fruits = self.amount_by_fruit.pop(client_id, {})
        for final_fruit_item in client_fruits.values():
            for exchange in self.data_output_exchanges:
                exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, final_fruit_item.fruit, final_fruit_item.amount]
                    )
                )

        logging.info(f"Sending EOF for client {client_id} to aggregation")
        for exchange in self.data_output_exchanges:
            exchange.send(message_protocol.internal.serialize([client_id]))

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 3:
            self._process_data(*fields)
        else:
            self._broadcast_eof(*fields)
        ack()

    def process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        self._process_eof(fields[0])
        ack()

    def start(self):
        eof_input_queue_name = f"{SUM_PREFIX}_{ID}_eof"
        self.input_queue.start_consuming(
            self.process_message,
            additional_sources=[(eof_input_queue_name, self.process_eof_message)]
        )

    def close(self):
        self.input_queue.close()
        for exchange in self.data_output_exchanges:
            exchange.close()
        for eof_queue in self.eof_output_queues:
            eof_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    try:
        sum_filter.start()
    except middleware.MessageMiddlewareDisconnectedError:
        logging.error("Lost connection to the message broker")
        return 1
    except middleware.MessageMiddlewareMessageError:
        logging.error("Internal middleware error")
        return 1
    finally:
        try:
            sum_filter.close()
        except middleware.MessageMiddlewareCloseError:
            logging.error("Error closing middleware connections")
    return 0


if __name__ == "__main__":
    main()
