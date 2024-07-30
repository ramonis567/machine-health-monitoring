import os
import json
import time
import threading
import paho.mqtt.client as paho
from paho import mqtt
from influxdb_client_3 import InfluxDBClient3, Point
from datetime import datetime, timedelta

# Carregar configurações do arquivo config.json
with open("config.json", "r") as f:
    config = json.load(f)

machine_id = config["machine_id"]
sensors = config["sensors"]

# Configurações do InfluxDB
influx_token = "DURFi7PxQy5jY2CJSseIpn7VDlrk-TEYbh5bWWguQeWIrn-PjfZo6nde4DQ_pm-sUDI5GCMvypkHvAR0e9JVOw=="
if not influx_token:
    raise ValueError("InfluxDB token not found in environment variables")
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

# Função de logging para imprimir informações
def log(message):
    print(f"[{datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')}] {message}")

# Callback para processar mensagens MQTT
def on_message(client, userdata, message):
    topic = message.topic
    payload = json.loads(message.payload.decode())
    
    log(f"Received message on topic {topic}: {json.dumps(payload, indent=2)}")
    
    handle_sensor_data(topic, payload)

def handle_sensor_data(topic, data):
    machine_id, sensor_id = parse_topic(topic)
    timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    value = data["value"]
    
    log(f"Handling data from machine {machine_id}, sensor {sensor_id} at {timestamp}: value={value}")
    
    persist_data(machine_id, sensor_id, timestamp, value)
    check_inactivity(machine_id, sensor_id, timestamp)
    custom_processing(machine_id, sensor_id, timestamp, value)

def persist_data(machine_id, sensor_id, timestamp, value):
    point = Point(sensor_id).tag("machine_id", machine_id).field("value", value).time(timestamp)
    bucket = f"{sensor_id}_data"
    log(f"Persisting data to InfluxDB bucket {bucket} for {machine_id}/{sensor_id} at {timestamp}: value={value}")
    try:
        client_DB.write(database=bucket, record=point)
    except Exception as e:
        log(f"Failed to write to InfluxDB: {e}")

def check_inactivity(machine_id, sensor_id, timestamp):
    data_interval = data_intervals.get((machine_id, sensor_id), 5)  # Usar 5 segundos como padrão se não encontrado
    inactive_threshold = timedelta(seconds=10 * data_interval)  # 10 períodos de tempo previstos

    if (machine_id, sensor_id) in last_seen:
        last_seen_time = last_seen[(machine_id, sensor_id)]
        if timestamp - last_seen_time > inactive_threshold:
            log(f"Inactive alarm triggered for {machine_id}/{sensor_id}")
            raise_alarm(machine_id, "inactive")
    last_seen[(machine_id, sensor_id)] = timestamp

def custom_processing(machine_id, sensor_id, timestamp, value):
    # Implementar processamento personalizado (exemplo: média móvel)
    log(f"Custom processing for {machine_id}/{sensor_id} at {timestamp}: value={value}")
    pass

def raise_alarm(machine_id, alarm_type):
    point = Point(alarm_type).tag("machine_id", machine_id).field("value", 1).time(datetime.utcnow())
    bucket = f"{machine_id}_alarms"
    log(f"Raising alarm for {machine_id}: {alarm_type}")
    try:
        client_DB.write(database=bucket, record=point)
    except Exception as e:
        log(f"Failed to raise alarm: {e}")

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
    log(f"CONNACK received with code {rc}")

# Criar e iniciar uma thread para cada tópico de sensor
for sensor in sensors:
    sensor_id = sensor["sensor_id"]
    data_interval = sensor["data_interval"]
    topic = f"/sensors/{machine_id}/{sensor_id}"

    # Armazenar o data_interval para calcular o inactive_threshold dinamicamente
    data_intervals[(machine_id, sensor_id)] = data_interval / 1000  # Converter para segundos

    log(f"Creating thread to subscribe to topic {topic} with data interval {data_interval} ms")
    thread = threading.Thread(target=subscribe_to_topic, args=(topic,))
    thread.start()
