import pika
import ssl
import json
import time
import logging

# Load configuration from external JSON file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

rabbitmq_host = config['rabbitmq_host']
rabbitmq_port = config['rabbitmq_port']
rabbitmq_user = config['rabbitmq_user']
rabbitmq_password = config['rabbitmq_password']
rabbitmq_vhost = config['rabbitmq_vhost']
queue_name = config['queue_name']
pause_duration = config['pause_duration']
max_retries = config['max_retries']
base_retry_interval = config['base_retry_interval']
queue_arguments = config['queue_arguments']

ca_certificate = config['ca_certificate']
client_certificate = config['client_certificate']
client_key = config['client_key']

# TLS parameters
context = ssl.create_default_context(cafile=ca_certificate)
context.load_cert_chain(certfile=client_certificate, keyfile=client_key)

# Disable hostname verification
context.check_hostname = False

# Disable certificate verification
context.verify_mode = ssl.CERT_NONE

credentials = pika.PlainCredentials(rabbitmq_user, rabbitmq_password)

parameters = pika.ConnectionParameters(
    host=rabbitmq_host,
    port=rabbitmq_port,
    virtual_host=rabbitmq_vhost,
    ssl_options=pika.SSLOptions(context),
    credentials=credentials
)

def establish_connection():
    retries = 0
    while retries < max_retries:
        try:
            connection = pika.BlockingConnection(parameters)
            return connection
        except pika.exceptions.AMQPConnectionError as e:
            retries += 1
            retry_interval = base_retry_interval * (2 ** (retries - 1))
            logging.error(f"Connection attempt {retries} failed: {e}. Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)
    raise Exception("Maximum retry attempts reached. Could not connect to RabbitMQ.")

def main():
    try:
        while True:
            connection = establish_connection()
            channel = connection.channel()
            channel.queue_declare(queue=queue_name, durable=True, arguments=queue_arguments)
            message = 'Hello, RabbitMQ with TLS!'
            channel.basic_publish(
                exchange='',
                routing_key=queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # make message persistent
                )
            )
            print(f" [x] Sent '{message}'")
            time.sleep(pause_duration)
            connection.close()
    except KeyboardInterrupt:
        print('Interrupted!')

if __name__ == "__main__":
    main()

