import pika
import os

from producer_interface import mqProducerInterface
class mqProducer(mqProducerInterface):
    def __init__(self, routing_key: str, exchange_name: str) -> None: 
        # saving variables
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        # calling setupRMQConnection
        self.setupRMQConnection()


    def setupRMQConnection(self) -> None:
        #setting up connection
        con_params = pika.URLParameters(os.environ['AMQP_URL'])
        self.connection = pika.BlockingConnection(parameters=con_params)
        #establishing channel
        self.channel = self.connection.channel()
        #creating exchange if not present
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='direct')

    def publishOrder(self, message: str) -> None:
        #basic publish to exchange
        self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_key, body=message)

        #closing channel
        self.channel.close()

        #closing connection
        self.connection.close()
        