<<<<<<< HEAD
<<<<<<< HEAD
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
=======
import os
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
import os
from confluent_kafka import Producer
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
import csv
from time import sleep
from typing import Dict

<<<<<<< HEAD
<<<<<<< HEAD

def load_avro_schema_from_file():
    key_schema = avro.load("taxi_ride_key.avsc")
    value_schema = avro.load("taxi_ride_value.avsc")

    return key_schema, value_schema
=======
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import RideRecordKey, ride_record_key_to_dict
from ride_record import RideRecord, ride_record_to_dict
from settings import RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, \
    SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField
from ride_record_key import RideRecordKey, ride_record_key_to_dict
from ride_record import RideRecord, ride_record_to_dict
from typing import Dict
from settings import RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, \
    SCHEMA_REGISTRY_URL, BOOTSTRAP_SERVERS, INPUT_DATA_PATH, KAFKA_TOPIC
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))


def delivery_report(err, msg):
    if err is not None:
        print("Delivery failed for record {}: {}".format(msg.key(), err))
        return
    print('Record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


class RideAvroProducer:
    def __init__(self, props: Dict):
        # Schema Registry and Serializer-Deserializer Configurations
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        self.key_serializer = AvroSerializer(schema_registry_client, key_schema_str, ride_record_key_to_dict)
        self.value_serializer = AvroSerializer(schema_registry_client, value_schema_str, ride_record_to_dict)

<<<<<<< HEAD
<<<<<<< HEAD
    file = open('data/rides.csv')
=======
        # Producer Configuration
        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
        # Producer Configuration
        producer_props = {'bootstrap.servers': props['bootstrap.servers']}
        self.producer = Producer(producer_props)
        self.topic = KAFKA_TOPIC
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))

    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    @staticmethod
    def delivery_report(err, msg):
        if err is not None:
            print("Delivery failed for record {}: {}".format(msg.key(), err))
            return
        print('Record {} successfully produced to {} [{}] at offset {}'.format(
            msg.key(), msg.topic(), msg.partition(), msg.offset()))

<<<<<<< HEAD
<<<<<<< HEAD
        producer.flush()
=======
=======
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
    @staticmethod
    def read_records(resource_path: str):
        ride_records, ride_keys = [], []
        with open(resource_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)  # skip the header
            for row in reader:
                ride_records.append(RideRecord(arr=[row[0], row[3], row[4], row[9], row[16]]))
                ride_keys.append(RideRecordKey(vendor_id=int(row[0])))
        return zip(ride_keys, ride_records)

<<<<<<< HEAD
    def publish(self, topic: str, records: [RideRecordKey, RideRecord]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.produce(topic=topic,
                                      key=self.key_serializer(key, SerializationContext(topic=topic,
                                                                                        field=MessageField.KEY)),
                                      value=self.value_serializer(value, SerializationContext(topic=topic,
=======
    def publish(self, records: [RideRecordKey, RideRecord]):
        for key_value in records:
            key, value = key_value
            try:
                self.producer.produce(topic=self.topic,
                                      key=self.key_serializer(key, SerializationContext(topic=self.topic,
                                                                                        field=MessageField.KEY)),
                                      value=self.value_serializer(value, SerializationContext(topic=self.topic,
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
                                                                                              field=MessageField.VALUE)),
                                      on_delivery=delivery_report)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Exception while producing record - {value}: {e}")

        self.producer.flush()
<<<<<<< HEAD
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
        sleep(1)


if __name__ == "__main__":
<<<<<<< HEAD
<<<<<<< HEAD
    send_record()
=======
=======
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,
        'schema.key': RIDE_KEY_SCHEMA_PATH,
        'schema.value': RIDE_VALUE_SCHEMA_PATH
    }
    producer = RideAvroProducer(props=config)
    ride_records = producer.read_records(resource_path=INPUT_DATA_PATH)
<<<<<<< HEAD
    producer.publish(topic=KAFKA_TOPIC, records=ride_records)
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
    producer.publish(records=ride_records)
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
