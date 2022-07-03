import paho.mqtt.client as mqtt
from pykafka import KafkaClient
import time
import json

mqtt_broker = "172.20.10.9"
mqtt_client = mqtt.Client("BridgeMQTT2Kafka")
mqtt_client.connect(mqtt_broker)

kafka_client = KafkaClient(hosts="localhost:9092")
kafka_topic = kafka_client.topics['ecg']
kafka_temp = kafka_client.topics['temp']
kafka_bp = kafka_client.topics['bp']

kafka_producer_temp = kafka_temp.get_sync_producer()
kafka_producer = kafka_topic.get_sync_producer()
kafka_producer_bp = kafka_bp.get_sync_producer()


def on_message(client, userdata, message):
    msg_payload = message.payload
    # msg_payload = json.loads(msg_payload)
    print("Received MQTT message: ", msg_payload)
    msg_payload = json.loads(msg_payload)
    type = msg_payload["type"]
    value=msg_payload["value"]
    
    print("Received MQTT message to json message: ", value)
    print("type: ", type)
    print("value: ", value)

    if(type=="temp"):
        # print('producing in temp**')
        value = {"temperature": value}
        print('new value', value)
        kafka_producer_temp.produce(json.dumps(value).encode('utf-8'))
       
       

    elif(type=="ecg"):
        kafka_producer.produce(json.dumps(value).encode('utf-8'))
        print('producing in ecg**', type, value)
       

        
    elif(type=="bp"):
        

        kafka_producer_bp.produce(json.dumps(value).encode('utf-8'))
        print('producing in bp**', type, value)

    

    # print(  msg_payload )

mqtt_client.loop_start()
mqtt_client.subscribe("temp")
mqtt_client.subscribe("ecg")
mqtt_client.subscribe("bp")
mqtt_client.on_message = on_message




time.sleep(100)
mqtt_client.loop_end()
