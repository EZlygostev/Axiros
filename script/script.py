import pika, time, logging
import json
import time
import requests
import os
from dotenv import load_dotenv

load_dotenv()

connection_params = pika.ConnectionParameters(
        host = os.getenv('BROKER_HOST'),
        port = os.getenv('BROKER_PORT'),
        virtual_host='/',
        credentials=pika.PlainCredentials(
            username = os.getenv('BROKER_USER'),
            password = os.getenv('BROKER_PASSWORD'))
        )

def call_service_a(device_id, parameters):
    try:
        response = requests.post(f'http://service_a:5002/api/v1/equipment/cpe/{device_id}', json=parameters, timeout=90)
        if response.status_code == 200:
            return "Completed"
        else:
            return "Internal provisioning exception"
    except Exception:
        return "Internal provisioning exception"

def process_tasks():
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='tasks')

    def callback(ch, method, properties, body):
        task = json.loads(body)
        task_id = task['taskId']
        device_id = task['id']
        parameters = task['body']
        
        status = call_service_a(device_id, parameters)
        
        result = {"taskId": task_id, "status": status}
        result_connection = pika.BlockingConnection(connection_params)
        result_channel = result_connection.channel()
        result_channel.queue_declare(queue='results')
        
        result_channel.basic_publish(exchange='', routing_key='results', body=json.dumps(result))
        result_connection.close()

    channel.basic_consume(queue='tasks', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    while True:
        try:
            connection = pika.BlockingConnection(connection_params)
            break
        except:
            time.sleep(20)
            continue
    process_tasks()
