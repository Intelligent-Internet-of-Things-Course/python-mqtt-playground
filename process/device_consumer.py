# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

import paho.mqtt.client as mqtt
from model.device_descriptor import DeviceDescriptor
from model.message_descriptor import MessageDescriptor
import json


# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    mqtt_client.subscribe(device_info_topic)
    print("Subscribed to: " + device_info_topic)
    mqtt_client.subscribe(data_topic)
    print("Subscribed to: " + data_topic)

# Define a callback method to receive asynchronous messages
def on_message(client, userdata, message):
    # If the received topic match the Info Subscription Filter
    if mqtt.topic_matches_sub(device_info_topic, message.topic):
       handle_device_info_message(message)
    # If the received topic match the Telemetry Subscription Filter
    elif mqtt.topic_matches_sub(data_topic, message.topic):
        handle_device_telemetry_message(message)
    else:
        print("Unmanaged Topic !")

def handle_device_info_message(message):
    message_payload = str(message.payload.decode("utf-8"))
    device_descriptor = DeviceDescriptor(**json.loads(message_payload))
    print(f"Received IoT Message (Retained:{message.retain}): Topic: {message.topic} DeviceId: {device_descriptor.deviceId} Manufacturer: {device_descriptor.producer} SoftwareVersion: {device_descriptor.softwareVersion}")

def handle_device_telemetry_message(message):
    message_payload = str(message.payload.decode("utf-8"))
    message_descriptor = MessageDescriptor(**json.loads(message_payload))
    print(f"Received IoT Message: Topic: {message.topic} Timestamp: {message_descriptor.timestamp} Type: {message_descriptor.type} Value: {message_descriptor.value}")

# Configuration variables
client_id = "clientId0001-Consumer"
broker_ip = "127.0.0.1"
broker_port = 1883
device_info_topic = "device/+/info"
data_topic = "device/+/sensor/#"
message_limit = 1000

# Create a new MQTT Client
mqtt_client = mqtt.Client(client_id)

# Attack Paho OnMessage Callback Method
mqtt_client.on_message = on_message
mqtt_client.on_connect = on_connect

# Connect to the target MQTT Broker
mqtt_client.connect(broker_ip, broker_port)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
mqtt_client.loop_forever()
