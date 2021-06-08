import threading
import logging
from typing import Optional
from kombu.mixins import ConsumerMixin
from kombu import Connection
from reiter.amqp.mq import AMQPCenter, AMQP


class Worker(ConsumerMixin):

    connection: Optional[Connection] = None

    def __init__(self, amqpcenter: AMQPCenter, url, app):
        self.url = url
        self.app = app
        self.amqpcenter = amqpcenter
        self.thread = threading.Thread(target=self.__call__)

    def get_consumers(self, Consumer, channel):
        return list(
            self.amqpcenter.consumers(Consumer, channel, app=self.app))

    def __call__(self):
        try:
            with Connection(self.url) as conn:
                self.connection = conn
                self.run()
        finally:
            self.connection = None

    def start(self):
        self.thread.start()

    def stop(self):
        self.should_stop = True
        logging.info("Quitting MQ thread.")
        self.thread.join()
