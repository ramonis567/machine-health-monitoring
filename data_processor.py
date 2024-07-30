import os
import json
import time
import threading
import paho.mqtt.client as paho
from paho import mqtt
from influxdb_client_3 import InfluxDBClient3, Point
from datetime import datetime, timedelta
from collections import deque

# Carregar configurações do arquivo config.json
with open("config.json", "r") as f:
    config = json.load(f)

machine_id = config["machine_id"]
sensors = config["sensors"]

# Configurações do InfluxDB
influx_token = "DURFi7PxQy5jY2CJSseIpn7VDlrk-TEYbh5bWWguQeWIrn-PjfZo6nde4DQ_pm-sUDI5GCMvypkHvAR0e9JVOw=="
influx_org = "EngContrAut"
influx_host = "https://us-east-1-1.aws.cloud2.influxdata.com"
client_DB = InfluxDBClient3(host=influx_host, token=influx_token, org=influx_org)

# Configurações do MQTT
mqtt_config = config["mqtt"]
mqtt_broker = mqtt_config["broker_url"]
mqtt_port = mqtt_config["broker_port"]
mqtt_username = mqtt_config["username"]
mqtt_password = mqtt_config["password"]

# Dicionário para armazenar o último timestamp de cada sensor e o data_interval
last_seen = {}
data_intervals = {}

# Callback para processar mensagens MQTT
def on_message(client, userdata, message):
    topic = message.topic
    payload = json.loads(message.payload.decode())
    
    # print(f"Received message: {topic} -> {json.dumps(payload, indent=2)}")

    handle_sensor_data(topic, payload)

def handle_sensor_data(topic, data):
    machine_id, sensor_id = parse_topic(topic)
    timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    value = data["value"]
    
    print(f"Data: {machine_id} - Sensor {sensor_id} at {timestamp}: value={value}")
    
    data_to_db(machine_id, sensor_id, timestamp, value)
    check_inactivity(machine_id, sensor_id, timestamp)
    custom_processing(machine_id, sensor_id, timestamp, value)

def data_to_db(machine_id, sensor_id, timestamp, value):
    point = Point(sensor_id).tag("machine_id", machine_id).field("value", value).time(timestamp)
    # print(f"Saving data to InfluxDB bucket {sensor_id}_data for {machine_id} - {sensor_id} at {timestamp}: value={value}")

    try:
        client_DB.write(database=f"{sensor_id}_data", record=point)
    except Exception as e:
        print(f"Failed to write to InfluxDB: {e}")

def check_inactivity(machine_id, sensor_id, timestamp):
    data_interval = data_intervals.get((machine_id, sensor_id), 5)  # Usar 5 segundos como padrão se não encontrado
    inactive_threshold = timedelta(seconds=10 * data_interval)  # 10 períodos de tempo previstos

    if (machine_id, sensor_id) in last_seen:
        last_seen_time = last_seen[(machine_id, sensor_id)]
        if timestamp - last_seen_time > inactive_threshold:
            print(f"Inactive alarm triggered for {machine_id}/{sensor_id}")
            raise_alarm(machine_id, "inactive")
    last_seen[(machine_id, sensor_id)] = timestamp

def custom_processing(machine_id, sensor_id, timestamp, value):
    # if (machine_id, sensor_id) not in sensor_data_queues:
    #     sensor_data_queues[(machine_id, sensor_id)] = deque(maxlen=5)
    
    # sensor_data_queues[(machine_id, sensor_id)].append(value)
    # moving_average = sum(sensor_data_queues[(machine_id, sensor_id)]) / len(sensor_data_queues[(machine_id, sensor_id)])
    
    # print(f"Custom processing for {machine_id}/{sensor_id} at {timestamp}: value={value}, moving average={moving_average}")
    
    # point = Point(f"{sensor_id}_moving_average").tag("machine_id", machine_id).field("value", moving_average).time(timestamp)
    # bucket = f"{sensor_id}_med_movel"
    # try:
    #     client_DB.write(database=bucket, record=point)
    # except Exception as e:
    #     print(f"Failed to write moving average to InfluxDB: {e}")
    pass

def raise_alarm(machine_id, alarm_type):
#     point = Point(alarm_type).tag("machine_id", machine_id).field("value", 1).time(datetime.utcnow())
#     bucket = f"{machine_id}_alarms"
#     print(f"Raising alarm for {machine_id}: {alarm_type}")
#     try:
#         client_DB.write(database=bucket, record=point)
#     except Exception as e:
#         log(f"Failed to raise alarm: {e}")
    pass

def parse_topic(topic):
    parts = topic.split('/')
    return parts[2], parts[3]

# Função para subscrever a um tópico específico em uma thread separada
def subscribe_to_topic(topic):
    client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
    client.on_connect = on_connect
    client.on_message = on_message

    client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
    client.username_pw_set(mqtt_username, mqtt_password)
    client.connect(mqtt_broker, mqtt_port)

    client.subscribe(topic)
    client.loop_forever()

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"CONNACK received with code {rc}")

# Criar e iniciar uma thread para cada tópico de sensor
for sensor in sensors:
    sensor_id = sensor["sensor_id"]
    data_interval = sensor["data_interval"]
    topic = f"/sensors/{machine_id}/{sensor_id}"

    # Armazenar o data_interval para calcular o inactive_threshold dinamicamente
    data_intervals[(machine_id, sensor_id)] = data_interval / 1000  # Converter para segundos

    thread = threading.Thread(target=subscribe_to_topic, args=(topic,))
    thread.start()
