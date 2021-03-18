import abc
from typing import List, ClassVar


class CustomConsumer(abc.ABC):
    queues: ClassVar[List[str]]
    accept: ClassVar[List[str]]

    def __init__(self, **context):
        self.context = context

    @abc.abstractmethod
    def __call__(self, body, message):
        pass
