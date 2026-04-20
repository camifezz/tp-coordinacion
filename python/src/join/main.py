import os
import logging

from common import middleware, message_protocol

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        self.top_count = {}   # {client_id: int}
        self.partial_tops = {}  # {client_id: [(fruit, amount), ...]}

    def process_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id, fruit_top = fields
        count = self.top_count.get(client_id, 0) + 1
        self.top_count[client_id] = count
        self.partial_tops.setdefault(client_id, []).extend(fruit_top)
        logging.info(f"Received top {count}/{AGGREGATION_AMOUNT} for client {client_id}")

        if count < AGGREGATION_AMOUNT:
            ack()
            return

        del self.top_count[client_id]
        all_fruits = self.partial_tops.pop(client_id)
        merged = sorted(all_fruits, key=lambda x: x[1], reverse=True)[:TOP_SIZE]
        self.output_queue.send(message_protocol.internal.serialize([client_id, merged]))
        ack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def close(self):
        self.input_queue.close()
        self.output_queue.close()


def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    try:
        join_filter.start()
    except middleware.MessageMiddlewareDisconnectedError:
        logging.error("Lost connection to the message broker")
        return 1
    except middleware.MessageMiddlewareMessageError:
        logging.error("Internal middleware error")
        return 1
    finally:
        try:
            join_filter.close()
        except middleware.MessageMiddlewareCloseError:
            logging.error("Error closing middleware connections")
    return 0


if __name__ == "__main__":
    main()
