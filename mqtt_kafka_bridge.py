import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time
import json

mqtt_broker = "172.20.10.9"
mqtt_client = mqtt.Client("BridgeMQTT2Kafka")
mqtt_client.connect(mqtt_broker)

kafka_client = KafkaClient(hosts="localhost:9092")
kafka_topic = kafka_client.topics['ecg']
kafka_producer = kafka_topic.get_sync_producer()

def on_message(client, userdata, message):
    msg_payload = message.payload
    # msg_payload = json.loads(msg_payload)
    print("Received MQTT message: ", msg_payload)
    msg_payload = json.loads(msg_payload)
    value=msg_payload["value"]
    print("Received MQTT message to json message: ", value)


    kafka_producer.produce(json.dumps(value).encode('utf-8'))
    # print(  msg_payload )

mqtt_client.loop_start()
mqtt_client.subscribe("ecg")
mqtt_client.on_message = on_message
time.sleep(100)
mqtt_client.loop_end()