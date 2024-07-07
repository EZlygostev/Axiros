from flask import Flask, request, jsonify
import pika
import uuid
import re
import threading
import json
from collections import defaultdict
import os
import time
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
app = Flask(__name__)

task_statuses = defaultdict(lambda: "Task is still running")

@app.route('/api/v1/equipment/cpe/<string:id>', methods=['POST'])
def create_task(id):
    if not re.match(r'^[a-zA-Z0-9]{6,}$', id):
        return jsonify({"code": 404, "message": "The requested equipment is not found"}), 404
    
    body = request.get_json()
    task_id = str(uuid.uuid4())
    task_statuses[task_id] = "Task is still running"
    
    message = {"id": id, "body": body, "taskId": task_id}

    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='tasks')
    
    channel.basic_publish(exchange='', routing_key='tasks', body=json.dumps(message))
    connection.close()
    
    return jsonify({"code": 200, "taskId": task_id}), 200

@app.route('/api/v1/equipment/cpe/<string:id>/task/<string:task>', methods=['GET'])
def get_task_status(id, task):
    status = task_statuses.get(task, "The requested task is not found")
    if status == "The requested task is not found":
        return jsonify({"code": 404, "message": status}), 404
    elif status == "Task is still running":
        return jsonify({"code": 200, "message": status}), 200
    else:
        return jsonify({"code": 200, "message": status}), 200

def listen_for_results():
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()
    channel.queue_declare(queue='results')

    def callback(ch, method, properties, body):
        result = json.loads(body)
        task_id = result['taskId']
        status = result['status']
        task_statuses[task_id] = status
    
    channel.basic_consume(queue='results', on_message_callback=callback, auto_ack=True)
    channel.start_consuming()

if __name__ == '__main__':
    while True:
        try:
            connection = pika.BlockingConnection(connection_params)
            break
        except:
            time.sleep(20)
            continue
    threading.Thread(target=listen_for_results).start()
    app.run(host='0.0.0.0', port=5001)
