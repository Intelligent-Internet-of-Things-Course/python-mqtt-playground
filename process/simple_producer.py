# For this example we rely on the Paho MQTT Library for Python
# You can install it through the following command: pip install paho-mqtt

from model.temperature_sensor import TemperatureSensor
import paho.mqtt.client as mqtt
import time


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))

#def on_publish(mqttc, obj, mid):
    #print("mid: " + str(mid))
    #pass

# Full MQTT client creation with all the parameters. The only one mandatory in the ClientId that should be unique
# mqtt_client = Client(client_id="", clean_session=True, userdata=None, protocol=MQTTv311, transport=”tcp”)

# Configuration variables
client_id = "clientId0001-Producer"
broker_ip = "127.0.0.1"
broker_port = 1883
default_topic = "sensor/temperature"
message_limit = 1000

mqtt_client = mqtt.Client(client_id)
mqtt_client.on_connect = on_connect
#mqtt_client.on_publish = on_publish

print("Connecting to "+ broker_ip + " port: " + str(broker_port))
mqtt_client.connect(broker_ip, broker_port)

mqtt_client.loop_start()

# Create Demo Temperature Sensor
temperature_sensor = TemperatureSensor()

# MQTT Paho Publish method with all the available parameters
# mqtt_client.publish(topic, payload=None, qos=0, retain=False)

for message_id in range(message_limit):

    # Simulate a new reading for the temperature sensor
    temperature_sensor.measure_temperature()

    # Read the new temperature value
    payload_string = temperature_sensor.temperature_value

    # Publish
    infot = mqtt_client.publish(default_topic, payload_string)
    infot.wait_for_publish()

    print(f"Message Sent: {message_id} Topic: {default_topic} Payload: {payload_string}")

    # Wait 1 second until the next publish
    time.sleep(1)

mqtt_client.loop_stop()
