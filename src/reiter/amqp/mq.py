import logging
from typing import Dict, List
from kombu import Exchange, Queue
from kombu.pools import producers
from reiter.amqp.meta import CustomConsumer


class AMQPCenter:

    exchange: Exchange = Exchange('object_events', type='topic')
    consumers: List[CustomConsumer]
    queues: Dict[str, Queue] = {
        'add': Queue(
            'add', exchange, routing_key='object.add'),
        'delete': Queue(
            'delete', exchange, routing_key='object.delete'),
        'update': Queue(
            'update', exchange, routing_key='object.update'),
    }

    def __init__(self, *consumers):
        self._consumers = list(consumers)

    def consumer(self, consumer: CustomConsumer):
        self._consumers.append(consumer)
        return consumer

    def consumers(self, cls, channel, **context):
        for consumer in self._consumers:
            yield cls(
                [self.queues[q] for q in consumer.queues],
                accept=consumer.accept,
                callbacks=[consumer(**context)]
            )


AMQP = AMQPCenter()


@AMQP.consumer
class TestConsumer(CustomConsumer):

    queues = ['add', 'update']
    accept = ['pickle', 'json']

    def __call__(self, body, message):
        logging.info("Got task body: %s", body)
        logging.info("Got task Message: %s", message)
        message.ack()
