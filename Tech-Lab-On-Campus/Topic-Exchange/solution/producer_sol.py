import os
from producer_interface import mqProducerInterface
import pika
import sys

class mqProducer(mqProducerInterface):
    
    def __init__(self, routing_key: str, exchange_name: str) -> None:
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        conParams = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=conParams)
        self.channel = self.connection.channel()
        self.channel.exchange_declare(
        exchange="Exchange Name", exchange_type="topic"
        )
        self.queue = self.channel.queue_declare(queue=self.routing_key)

    def publishOrder(self, message: str) -> None:
        self.channel.basic_publish(exchange=self.exchange_name,
                      routing_key=self.routing_key,
                      body=message)
        print(" [x] Delivered order")
        self.connection.close()
        print(" [x] Connection closed")
