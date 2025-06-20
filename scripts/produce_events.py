from confluent_kafka import Producer
import json, time

conf = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'MYDFB2FDNLVV2QLG',        
    'sasl.password': 'htVdfSueWUUkvefpM6i2Zu0p7xZ2QatIb+5L2hwQHpIyBY5K2IBb5YqeS0zbzgt1'     
}

producer = Producer(conf)

for i in range(20):
    event = {
        "event_id": f"evt_{i}",
        "event_type": "click",
        "timestamp": "2025-04-04T15:00:00",
        "value": 50 + i
    }
    producer.produce("stream-input", value=json.dumps(event))
    producer.poll(0)
    print(f" Sent: {event}")
    time.sleep(0.5)

producer.flush()
