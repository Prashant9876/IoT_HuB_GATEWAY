import os
from dotenv import load_dotenv

load_dotenv()

# MQTT
MQTT_BROKER = os.getenv("MQTT_BROKER")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_ACK_TOPIC_PREFIX = os.getenv("MQTT_ACK_TOPIC_PREFIX")

FARM_ID = os.getenv("FARM_ID")
DEVICE_ID = os.getenv("DEVICE_ID")