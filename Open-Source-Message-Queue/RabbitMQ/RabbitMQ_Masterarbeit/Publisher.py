import pika
from pika.exchange_type import ExchangeType
import myConnection

connection_parameters = pika.ConnectionParameters('localhost')

connection = pika.BlockingConnection(connection_parameters)

channel = connection.channel()

channel.exchange_declare(exchange='topic', exchange_type=ExchangeType.topic)


#message
first_message = myConnection.user

#Binding
first = channel.basic_publish(exchange='topic', routing_key='overpelt.dilisson.invoice', body=first_message)

#Ausgabe Nachricht
print(f"sent message: {first_message}")

#message
second_message = "This is my europe Order"

#Binding
second = channel.basic_publish(exchange='topic', routing_key='This.europe.order', body=second_message)

#Ausgabe Nachricht
print(f"sent message: {second_message}")

connection.close()