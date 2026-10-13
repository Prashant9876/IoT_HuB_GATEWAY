import json
import os
import time
import threading
import paho.mqtt.client as mqtt
from datetime import datetime, timezone ,timedelta
from config import  configure as config
from zoneinfo import ZoneInfo
import pytz
from database.sql_database import Tables ,mark_uploaded

# ========================
# ENV CONFIG
# ========================


MQTT_BROKER = config.MQTT_BROKER
MQTT_PORT = config.MQTT_PORT
MQTT_USERNAME = config.MQTT_USERNAME
MQTT_PASSWORD = config.MQTT_PASSWORD
FARM_ID = config.FARM_ID


MQTT_ACK_TOPIC_PREFIX = config.MQTT_ACK_TOPIC_PREFIX

PING_TOPIC = f"farm/IoT_HuB/TimeSync"
PING_INTERVAL = 60  # seconds

stop_event = threading.Event()

last_refresh = 0

mqtt_client = None
connected = False

def check_topic(table_name):
    if table_name  in ["THC", "WLD", "EMD", "LSD", "NSD"]:
        return f"farm/{FARM_ID}/sensor"
    else:
        print(f"⚠️ Unknown table name: {table_name}")
        return None

# ========================
# MQTT CALLBACKS
# ========================

def on_connect(client, userdata, flags, rc):
    global connected
    if rc == 0:
        connected = True
        print("MQTT connected")
        # Subscribe to all allowed farms
        # if config.ALLOWED_FARMS:  # make sure it's not empty
        #     for farm_id in config.ALLOWED_FARMS.keys():  # iterate over dict keys (farm IDs)
        #         topic = f"farm/{farm_id}/#"
        #         client.subscribe(topic)
        #         print("Subscribed to:", topic)
        # else:
        #     print("⚠️ ALLOWED_FARMS is empty. No subscriptions made.")
    else:
        print(" MQTT connection failed with code:", rc)

def on_disconnect(client, userdata, rc):
    global connected
    connected = False
    print("⚠ MQTT disconnected. Reconnecting...")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print("\n▶ MQTT Topic:", msg.topic)
        print("▶ MQTT Payload:", payload)
        topic_parts = msg.topic.split("/")

        if len(topic_parts) < 3:
            print("Invalid topic format:", msg.topic)
            return

        farm_id = topic_parts[1]
        topic_type = topic_parts[2]

        if farm_id not in config.ALLOWED_FARMS:
            print(f"Farm {farm_id} not allowed")
            return

        device_id = (
            payload.get("Device_Id")
            or payload.get("DeviceID")
            or payload.get("DeviceId")
        )

        if topic_type == "sensor":
            payload["type"] = "sensor"
            payload["timestamp"] = datetime.now(timezone.utc).isoformat()
            payload["Ist_timestamp"] = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
            payload["farm_id"] = farm_id

            if farm_id == "120" and device_id == "IFTHC1200000001":
                humidity = payload.get("Humidity")
                Co2 = payload.get("CO2")

                if isinstance(humidity, (int, float)):
                    payload["Humidity"] = round(humidity + 30, 2)
                    print(
                        f"Humidity corrected: {humidity} → "
                        f"{payload['Humidity']}"
                    )

                if isinstance(Co2, (int, float)):
                    payload["CO2"] = round(Co2/10,0)
                    print(f"CO2 corrected: {Co2} → {payload['CO2']}")   
                else:
                    print("Humidity key is missing or has an invalid value")


            if (farm_id == "120" and device_id == "IFNSD1200000001" ):
                EC_val= payload.get("EC Sensor")
                ph_val = payload.get("pH Sensor")
                cases = payload.get("cases")

                if EC_val == 0 or ph_val == 0 :
                    print(f"⚠️ Invalid sensor data from {device_id} in farm {farm_id}. EC: {EC_val}, pH: {ph_val}")
                    return

                if cases in ["before", "after"]:
                        cosmos_dev.store_to_mongo(device_id, payload["type"], payload)
                        return

        elif topic_type in {"actuator", "irrigation", "fertigation"}:
            payload["type"] = "actuator"
            payload["timestamp"] = datetime.now(timezone.utc).isoformat()
            payload["Ist_timestamp"] = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
            payload["farm_id"] = farm_id  # Example additional field for actuators
        # elif topic_type == "robot":
        #     payload["type"] = "robot"
        #     payload["timestamp"] = datetime.now(timezone.utc).isoformat()
        #     payload["farm_id"] = farm_id  # Example additional field for robots

        elif topic_type == "Info":
            redis_client.check_and_update_device_config(payload, farm_id)
            DNs = payload.get("DN")
            if DNs == "LCD":
                cosmos_dev.revert_schedule_detail(payload, farm_id, client)
                return
            if DNs == "AFC" or DNs == "FNC":
                cosmos_dev.revert_Fan_schedule_detail(payload, farm_id, client)
                return
            return
        elif topic_type == "api" or topic_type == "SSub":
            payload["farm_id"] = farm_id
            cosmos_dev.store_to_mongo("Backend_API", "APIs", payload)
            return
        elif topic_type in {"sensor_keepalive", "actuator_keepalive"}:
            if not device_id:
                print("Missing device identifier, skipping keepalive")
                return

            payload["type"] = "Keepalive"
            payload["createdAt"] = datetime.now(timezone.utc)
            payload["Ist_timestamp"] = datetime.now(ZoneInfo("Asia/Kolkata")).isoformat()
            payload["farm_id"] = farm_id
            cosmos_dev.store_to_mongo(device_id, payload["type"], payload)
            return
        else:
            print(f"Topic type {topic_type} not recognized")
            return

        redis_data = redis_client.store_device_data(device_id, payload, type=payload["type"], farm_Id=farm_id)
        cosmos_dev.store_to_mongo(device_id, payload["type"], payload)

    except Exception as e:
        print("Topic parsing error:", e)
    return


## publish function for sending messages to MQTT broker
def publish_message(topic, payload):
    if mqtt_client and connected:
        mqtt_client.publish(topic, json.dumps(payload))
        print(f"Published to {topic}: {payload}")
    else:
        print("⚠ MQTT not connected. Message not sent.")

# ========================
# MQTT DATA UPLOADER THREAD
# ========================

def Mqtt_data_uploader():
    table_index = 0
    while True:
        if not Tables:
            print("No tables available")
            stop_event.wait(5)
            continue
        table_name = Tables[table_index]  # Get the first table name
        print(f"Uploading data from table: {table_name}")
        try:
            rows = _read(table_name, uploaded=False, limit=10)
            if not rows:
                print(f"{table_name}: no pending data")
                continue
            
            for row in rows:
                payload_data = dict(row)
                payload_data["Packet_Id"] = f"{table_name}-{row['id']}"
                payload_data["Device_Id"] = payload_data.pop("device_id")
                payload_data["DN"] = table_name
                payload_data.pop("uploaded", None)
                payload = json.dumps(payload_data)
                print(payload)
                topic = check_topic(table_name)

                result = mqtt_client.publish(
                    topic=topic,
                    payload=payload,
                    qos=1
                )
                # Check that Paho accepted the publish request.
                if result.rc != mqtt.MQTT_ERR_SUCCESS:
                    print(f"MQTT publish not queued for ID: {row['id']}")
                    break
                deadline = time.monotonic() + 10
                while not result.is_published():
                    if stop_event.is_set():
                        break

                    if time.monotonic() >= deadline:
                        print(f"MQTT confirmation timeout for ID: {row['id']}")
                        break
                    time.sleep(0.05)

                if result.is_published():
                    mark_uploaded(table_name, [row["id"]])
                    print(f"Uploaded: {table_name}, record ID: {row['id']}")
                else:
                    print(f"Not confirmed; keeping record pending: {row['id']}")
                    break
                time.sleep(0.1)
        except Exception as error:
            print(f"Uploader error in {table_name}: {error}")

    table_index = (table_index + 1) % len(Tables)
    time.sleep(0.1)




# ========================
# HEARTBEAT THREAD
# ========================

def heartbeat():
    global mqtt_client, last_refresh
    time.sleep(5)  # Initial delay to allow MQTT connection to establish
    while True:
        if mqtt_client and connected:
            payload = {
                    "type": "heartbeat",
                    "farm_id": config.FARM_ID,
                    "utc_time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "ist_time": datetime.now(ZoneInfo("Asia/Kolkata")).isoformat(),
                    "local_time": datetime.now(ZoneInfo("Asia/Kolkata")).strftime("%Y-%m-%d %H:%M:%S"),
                    "epoch": int(time.time()),
                    "device_id": config.DEVICE_ID                    
                }
            result = mqtt_client.publish(PING_TOPIC, json.dumps(payload), qos=1)
            result.wait_for_publish()
            print(f"heartbeat send :- {result.rc} , {result.mid}")
            print("Heartbeat sent to:", PING_TOPIC)
        else:
            print("MQTT not connected. Heartbeat not sent.")
        time.sleep(PING_INTERVAL)

# ========================
# MQTT STARTER
# ========================
def start_mqtt_connection():
    global mqtt_client, last_refresh

    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_disconnect = on_disconnect
    mqtt_client.on_message = on_message

    # Username & password
    if MQTT_USERNAME and MQTT_PASSWORD:
        mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

    # Auto reconnect settings
    mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        print(" MQTT connection error:", e)
        raise SystemExit(1)

    # Start heartbeat thread

    threading.Thread(target=heartbeat, daemon=True).start()
    threading.Thread(target=Mqtt_data_uploader , daemon=True).start()

    print("🚀 MQTT client started with auto-reconnect & heartbeat")
    mqtt_client.loop_forever()
