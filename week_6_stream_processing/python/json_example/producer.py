import csv
import json
from typing import List, Dict
<<<<<<< HEAD
from kafka import KafkaProducer
from kafka.errors import KafkaTimeoutError

from ride import Ride
=======

from kafka.errors import KafkaTimeoutError

from ride import Ride
from kafka import KafkaProducer

>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
from settings import BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC


class JsonProducer(KafkaProducer):
    def __init__(self, props: Dict):
<<<<<<< HEAD
        self.producer = KafkaProducer(**props)
=======
        self.resources = './resources/rides.csv'
        self.producer = KafkaProducer(**props)
        self.topic = KAFKA_TOPIC
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))

    @staticmethod
    def read_records(resource_path: str):
        records = []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header row
            for row in reader:
                records.append(Ride(arr=row))
        return records

<<<<<<< HEAD
    def publish_rides(self, topic: str, messages: List[Ride]):
        for ride in messages:
            try:
                record = self.producer.send(topic=topic, key=ride.pu_location_id, value=ride)
=======
    def publish_rides(self, messages: List[Ride]):
        for ride in messages:
            try:
                record = self.producer.send(topic=self.topic, key=ride.pu_location_id, value=ride)
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
                print('Record {} successfully produced at offset {}'.format(ride.pu_location_id, record.get().offset))
            except KafkaTimeoutError as e:
                print(e.__str__())


if __name__ == '__main__':
    # Config Should match with the KafkaProducer expectation
    config = {
        'bootstrap_servers': BOOTSTRAP_SERVERS,
        'key_serializer': lambda key: str(key).encode(),
        'value_serializer': lambda x: json.dumps(x.__dict__, default=str).encode('utf-8')
    }
    producer = JsonProducer(props=config)
    rides = producer.read_records(resource_path=INPUT_DATA_PATH)
<<<<<<< HEAD
    producer.publish_rides(topic=KAFKA_TOPIC, messages=rides)
=======
    producer.publish_rides(messages=rides)
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
