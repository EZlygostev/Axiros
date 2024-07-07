from flask import Flask, request, jsonify
import time
import re

app = Flask(__name__)

@app.route('/api/v1/equipment/cpe/<string:id>', methods=['POST'])
def configure_device(id):
    if not re.match(r'^[a-zA-Z0-9]{6,}$', id):
        return jsonify({"code": 404, "message": "The requested equipment is not found"}), 404
    
    try:
        time.sleep(60) 
        return jsonify({"code": 200, "message": "success"}), 200
    except Exception as e:
        return jsonify({"code": 500, "message": "Internal provisioning exception"}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)
