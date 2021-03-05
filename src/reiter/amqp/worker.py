import threading
import logging
from typing import Optional
from kombu.mixins import ConsumerMixin
from kombu import Connection
from reiter.amqp.mq import AMQPCenter


class Worker(ConsumerMixin):

    connection: Optional[Connection] = None

    def __init__(self, app, config, amqpcenter: AMQPCenter = AMQP):
        self.app = app
        self.config = config
        self.amqpcenter = amqpcenter
        self.thread = threading.Thread(target=self.__call__)

    def get_consumers(self, Consumer, channel):
        consumers = list(
            self.amqpcenter.consumers(self.app, Consumer, channel))
        return consumers

    def __call__(self):
        try:
            with Connection(self.config.url) as conn:
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
