import logging
import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """
    Implementación de MessageMiddlewareQueue para RabbitMQ.
    Maneja comunicación punto a punto mediante una cola nombrada.
    Varios productores pueden enviar a la misma cola y varios
    consumidores compiten por los mensajes (cada mensaje lo recibe uno solo).
    """

    def __init__(self, host, queue_name):
        """
        Conecta a RabbitMQ y declara la cola.
        - host: dirección del broker RabbitMQ
        - queue_name: nombre de la cola a usar
        """
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
        # RabbitMQ espera el ack de cada mensaje antes de mandarle otro al consumidor
        self.channel.basic_qos(prefetch_count=1)

    def send(self, message):
        """
        Publica un mensaje en la cola.
        - message: contenido del mensaje a enviar
        Lanza MessageMiddlewareDisconnectedError si se perdió la conexión.
        Lanza MessageMiddlewareMessageError si ocurre un error interno.
        """
        try:
            self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
            logging.info(f"Sent to {self.queue_name}: {message}")
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback, additional_sources=None):
        """
        Comienza a escuchar mensajes de la cola de forma bloqueante.
        Por cada mensaje recibido invoca: on_message_callback(message, ack, nack)
          - message: cuerpo del mensaje
          - ack: función para confirmar que el mensaje se recibió correctamente (equivalente a un 200)
          - nack: función para indicar que algo falló procesando el mensaje (equivalente al 500)
        additional_sources: lista opcional de tuplas (queue_name, callback) para consumir
          colas adicionales en la misma conexión.
        Lanza MessageMiddlewareDisconnectedError si se perdió la conexión.
        Lanza MessageMiddlewareMessageError si ocurre un error interno.
        """
        def make_callback(queue_name, cb):
            def callback(ch, method, _properties, body):
                logging.info(f"Received from {queue_name}: {body}")
                ack  = lambda: ch.basic_ack(method.delivery_tag)
                nack = lambda: ch.basic_nack(method.delivery_tag)
                cb(body, ack, nack)
            return callback

        try:
            self.channel.basic_consume(queue=self.queue_name,
                                       auto_ack=False,
                                       on_message_callback=make_callback(self.queue_name, on_message_callback))
            if additional_sources:
                for (queue_name, cb) in additional_sources:
                    self.channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})
                    self.channel.basic_consume(queue=queue_name,
                                               auto_ack=False,
                                               on_message_callback=make_callback(queue_name, cb))
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        """
        Detiene la escucha de mensajes. Si no se estaba consumiendo, no tiene efecto.
        Lanza MessageMiddlewareDisconnectedError si se perdió la conexión.
        """
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def close(self):
        """
        Cierra el canal y la conexión con RabbitMQ.
        Lanza MessageMiddlewareCloseError si ocurre un error al cerrar.
        """
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError() from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """
    Implementación de MessageMiddlewareExchange para RabbitMQ.
    Maneja comunicación pub-sub mediante un exchange de tipo direct.
    Los mensajes se enrutan por routing keys: cada consumidor recibe
    los mensajes de las routing keys a las que está suscripto.
    A diferencia de la queue, todos los consumidores suscritos a una
    routing key reciben el mismo mensaje (broadcast por key).
    """

    def __init__(self, host, exchange_name, routing_keys):
        """
        Conecta a RabbitMQ y declara el exchange.
        - host: dirección del broker RabbitMQ
        - exchange_name: nombre del exchange a usar
        - routing_keys: lista de claves de enrutamiento a las que suscribirse al consumir
        """
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=exchange_name, exchange_type='direct')
        self.temp_queue_name = None

    def send(self, message):
        """
        Publica un mensaje en el exchange para cada routing key de self.routing_keys.
        - message: contenido del mensaje a enviar
        Lanza MessageMiddlewareDisconnectedError si se perdió la conexión.
        Lanza MessageMiddlewareMessageError si ocurre un error interno.
        """
        try:
            for routing_key in self.routing_keys:
                self.channel.basic_publish(exchange=self.exchange_name,
                                           routing_key=routing_key,
                                           body=message)
                logging.info(f"Sent to {self.exchange_name}/{routing_key}: {message}")
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def start_consuming(self, on_message_callback):
        """
        Crea una cola temporal con nombre único, la bindea al exchange
        para cada routing key de self.routing_keys, y comienza a escuchar.
        Por cada mensaje recibido invoca: on_message_callback(message, ack, nack)
          - message: cuerpo del mensaje
          - ack: función para confirmar el mensaje
          - nack: función para rechazar el mensaje
        Lanza MessageMiddlewareDisconnectedError si se perdió la conexión.
        Lanza MessageMiddlewareMessageError si ocurre un error interno.
        """
        try:
            result = self.channel.queue_declare(queue='', exclusive=True)
            self.temp_queue_name = result.method.queue
            for routing_key in self.routing_keys:
                self.channel.queue_bind(exchange=self.exchange_name,
                                        queue=self.temp_queue_name,
                                        routing_key=routing_key)
            def callback(ch, method, _properties, body):
                logging.info(f"Received from {self.exchange_name}: {body}")
                ack  = lambda: ch.basic_ack(method.delivery_tag)
                nack = lambda: ch.basic_nack(method.delivery_tag)
                on_message_callback(body, ack, nack)
            self.channel.basic_consume(queue=self.temp_queue_name, on_message_callback=callback, auto_ack=False)
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        """
        Detiene la escucha de mensajes. Si no se estaba consumiendo, no tiene efecto.
        Lanza MessageMiddlewareDisconnectedError si se perdió la conexión.
        """
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError() from e

    def close(self):
        """
        Cierra la conexión con RabbitMQ (la cola temporal se elimina automáticamente
        al cerrar la conexión por ser exclusive=True).
        Lanza MessageMiddlewareCloseError si ocurre un error al cerrar.
        """
        try:
            self.connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError() from e
