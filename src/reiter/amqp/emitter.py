from kombu import Exchange, Connection
from kombu.pools import producers


class AMQPEmitter:

    def __init__(self, exchange,
                 url="amqp://localhost:5672//",
                 serializer="json"):
        self.url = url
        self.serializer = serializer
        self.exchange = exchange

    def send(self, payload, key):
        with Connection(self.url) as conn:
            with producers[conn].acquire(block=True) as producer:
                producer.publish(
                    payload,
                    serializer=self.serializer,
                    exchange=self.exchange,
                    declare=[self.exchange],
                    routing_key=key,
                )
