import time
import json
import random
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

for multiplier in range(100):
    print(f'sending data, multiplier = {multiplier}')
    for i in range(100):
        device_id = f'd{i % 3}'
        if device_id == 'd0':
            feature = 'f1'
        if device_id == 'd1':
            feature = 'f2'
        if device_id == 'd2':
            feature = 'f3' if random.random() < 0.5 else 'f4'
        producer.send('test',
                      key=device_id.encode('utf-8'),
                      value={'name': feature,
                             'device': device_id,
                             'value': multiplier * 100 + i,
                             'timestamp': multiplier * 100 + i})
        producer.flush()
    time.sleep(0.8)
