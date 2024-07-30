import json
import threading
import time
import uuid
import paho.mqtt.client as paho
from paho import mqtt
from datetime import datetime

# Função para ler o arquivo de configuração
def read_config(filepath):
    with open(filepath, 'r') as file:
        config = json.load(file)
    return config

# Função para obter o uso da CPU
def get_cpu_usage():
    try:
        with open('/proc/stat', 'r') as f:
            lines = f.readlines()
    except IOError:
        print("Error opening /proc/stat")
        return -1.0

    for line in lines:
        if line.startswith('cpu '):
            cpu_times = line.split()[1:]
            cpu_times = list(map(int, cpu_times))
            if len(cpu_times) < 4:
                return -1.0
            idle_time = cpu_times[3]
            total_time = sum(cpu_times)
            cpu_usage = 1.0 - (idle_time / total_time)
            cpu_usage *= 100.0
            return cpu_usage
    return -1.0

# Função para obter o uso da RAM
def get_ram_usage():
    try:
        with open('/proc/meminfo', 'r') as meminfo:
            m_total = 0
            m_avail = 0
            for line in meminfo:
                if line.startswith("MemTotal:"):
                    m_total = int(line.split()[1])
                elif line.startswith("MemAvailable:"):
                    m_avail = int(line.split()[1])
            if m_total == 0:
                raise ValueError("MemTotal is zero, unable to compute RAM usage.")
            return ((m_total - m_avail) / m_total) * 100.0
    except FileNotFoundError:
        print("Error opening /proc/meminfo")
        return -1.0
    except Exception as e:
        print(f"Error: {e}")
        return -1.0

# Função para obter o timestamp atual
def get_timestamp():
    now = datetime.utcnow()
    timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    return timestamp

# Funções de callback do MQTT
def on_connect(client, userdata, flags, rc, properties=None):
    print("CONNACK received with code %s." % rc)

def on_publish(client, userdata, mid, properties=None):
    # print("mid: " + str(mid))
    pass

# Publica uma mensagem inicial
def publish_initial_message(client, machine_id, sensors):
    initial_message = {
        "machine_id": machine_id,
        "sensors": sensors
    }
    client.publish("/sensor_monitors", json.dumps(initial_message), qos=1)

# Função para publicar dados do sensor
def publish_sensor_data(client, machine_id, sensor_id, get_data_func, interval):
    while True:
        timestamp_value = get_timestamp()
        data_value = get_data_func()

        data = {
            "timestamp": timestamp_value,
            "value": data_value
        }
        topic = f"/sensors/{machine_id}/{sensor_id}"
        client.publish(topic, json.dumps(data), qos=1)

        print("Publish: " + topic + " -> " + str(timestamp_value) + " -> " + str(data_value))

        time.sleep(interval / 1000)


config = read_config('config.json')

# Configuração do cliente MQTT
client = paho.Client(client_id="", userdata=None, protocol=paho.MQTTv5)
client.on_connect = on_connect

client.tls_set(tls_version=mqtt.client.ssl.PROTOCOL_TLS)
client.username_pw_set(config['mqtt']['username'], config['mqtt']['password'])
client.connect(config['mqtt']['broker_url'], config['mqtt']['broker_port'])

client.on_publish = on_publish

# Iniciar threads para cada sensor
threads = []
for sensor in config['sensors']:
    sensor_id = sensor['sensor_id']
    data_interval = sensor['data_interval']
    if sensor_id == "cpu_usage":
        get_data_func = get_cpu_usage
    elif sensor_id == "ram_usage":
        get_data_func = get_ram_usage
    else:
        continue

    thread = threading.Thread(target=publish_sensor_data, args=(client, config['machine_id'], sensor_id, get_data_func, data_interval))
    threads.append(thread)
    thread.start()

publish_initial_message(client, config['machine_id'], config['sensors'])
client.loop_forever()