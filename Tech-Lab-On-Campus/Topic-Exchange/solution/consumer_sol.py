from consumer_interface import mqConsumerInterface
import pika
import os
class mqConsumer(mqConsumerInterface):
    def __init__(self, binding_key, exchange_name, queue_name):
        self.binding_key = binding_key
        self.exchange_name = exchange_name
        self.queue_name = queue_name
        self.setupRMQConnection()
        print("Consumer Created")
    
    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        connection = pika.BlockingConnection(parameters=con_params)        # Establish Channel
        channel = connection.channel()
        channel.queue_declare(queue=self.queue_name)
        channel.exchange_declare(exchange=self.exchange_name)
        channel.queue_bind(
            queue=self.queue_name,
            routing_key=self.binding_key,
            exchange=self.exchange_name,
        )
        channel.basic_consume(
            self.queue_name, self.on_message_callback, auto_ack=False
        )
        self.channel = channel
        self.connection = connection
        # print("Connection Established")

    def on_message_callback(self, channel, method_frame, header_frame, body):
        channel.basic_ack(method_frame.delivery_tag, False)
        print(body)

    def startConsuming(self):
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.channel.start_consuming()

    def __del__(self):
        # print("Closing RMQ connection on destruction")
        self.channel.close()
        self.connection.close()
    

