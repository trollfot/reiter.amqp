import inspect
from functools import partial
from typing import Dict, List, Union, Type
from kombu import Queue
from reiter.amqp.meta import CustomConsumer


class AMQPCenter:

    consumers: List[Union[CustomConsumer, Type[CustomConsumer]]]
    queues: Dict[str, Queue]

    def __init__(self, queues, *consumers):
        self.queues = queues
        self._consumers = list(consumers)

    def consumer(self, consumer: CustomConsumer):
        self._consumers.append(consumer)
        return consumer

    def consumers(self, cls, channel, **context):
        for consumer in self._consumers:
            if inspect.isclass(consumer) or isinstance(consumer, partial):
                call = consumer(**context)
            else:
                call = consumer
            yield cls(
                [self.queues[q] for q in call.queues],
                accept=call.accept,
                callbacks=[call]
            )
