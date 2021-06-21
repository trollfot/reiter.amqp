import threading
import logging
from typing import Optional, Any
from kombu.mixins import ConsumerMixin
from kombu import Connection
from functools import partial
from reiter.amqp.mq import AMQPCenter, AMQP


class Consumer(ConsumerMixin):

    def __init__(self, url: str, amqpcenter: AMQPCenter, app: Any):
        self.url = url
        self.amqpcenter = amqpcenter
        self.app = app

    def get_consumers(self, consumer, channel):
        return list(
            self.amqpcenter.consumers(consumer, channel, app=self.app))

    def __call__(self):
        with Connection(self.url) as conn:
            try:
                self.connection = conn
                self.run()
            finally:
                self.connection = None


class Worker(threading.Thread):

    def __init__(self, app: Any, url: str, amqpcenter: AMQPCenter):
        self.runner = Consumer(url, amqpcenter, app)
        super().__init__(target=self.runner.__call__)

    def start(self):
        logging.info("Starting MQ thread.")
        super().start()

    def join(self):
        self.runner.should_stop = True
        logging.info("Quitting MQ thread.")
        super().join()
