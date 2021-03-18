import threading
import logging
from typing import Optional
from kombu.mixins import ConsumerMixin
from kombu import Connection
from reiter.amqp.mq import AMQPCenter, AMQP


class Worker(ConsumerMixin):

    connection: Optional[Connection] = None

    def __init__(self, config, amqpcenter: AMQPCenter = AMQP):
        self.context = {}
        self.config = config
        self.amqpcenter = amqpcenter
        self.thread = threading.Thread(target=self.__call__)

    def get_consumers(self, Consumer, channel):
        consumers = list(
            self.amqpcenter.consumers(Consumer, channel, **self.context))
        return consumers

    def __call__(self, **context):
        try:
            self.context = context
            with Connection(self.config.url) as conn:
                self.connection = conn
                self.run()
        finally:
            self.connection = None
            self.context = None

    def start(self):
        self.thread.start()

    def stop(self):
        self.should_stop = True
        logging.info("Quitting MQ thread.")
        self.thread.join()
