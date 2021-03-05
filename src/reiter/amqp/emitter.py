from kombu import Exchange, Connection


class AMQPEmitter:

    def __init__(self, config):
        self.config = config
        self.exchange = Exchange("object_events", type="topic")

    def send(self, payload, key):
        with Connection(self.config.url) as conn:
            with producers[conn].acquire(block=True) as producer:
                producer.publish(
                    payload,
                    serializer=self.config.serializer,
                    exchange=self.exchange,
                    declare=[self.exchange],
                    routing_key=key,
                )
