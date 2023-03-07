<<<<<<< HEAD
<<<<<<< HEAD
from confluent_kafka.avro import AvroConsumer
=======
import os
from typing import Dict, List
=======
import os
<<<<<<< HEAD
from typing import Dict
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
=======
from typing import Dict, List
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))

from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from ride_record_key import dict_to_ride_record_key
from ride_record import dict_to_ride_record
from settings import BOOTSTRAP_SERVERS, SCHEMA_REGISTRY_URL, \
    RIDE_KEY_SCHEMA_PATH, RIDE_VALUE_SCHEMA_PATH, KAFKA_TOPIC
<<<<<<< HEAD
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))


class RideAvroConsumer:
    def __init__(self, props: Dict):

        # Schema Registry and Serializer-Deserializer Configurations
        key_schema_str = self.load_schema(props['schema.key'])
        value_schema_str = self.load_schema(props['schema.value'])
        schema_registry_props = {'url': props['schema_registry.url']}
        schema_registry_client = SchemaRegistryClient(schema_registry_props)
        self.avro_key_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                      schema_str=key_schema_str,
                                                      from_dict=dict_to_ride_record_key)
        self.avro_value_deserializer = AvroDeserializer(schema_registry_client=schema_registry_client,
                                                        schema_str=value_schema_str,
                                                        from_dict=dict_to_ride_record)

<<<<<<< HEAD
<<<<<<< HEAD
    while(True):
        try:
            message = consumer.poll(5)
        except Exception as e:
            print(f"Exception while trying to poll messages - {e}")
        else:
            if message:
                print(f"Successfully poll a record from "
                      f"Kafka topic: {message.topic()}, partition: {message.partition()}, offset: {message.offset()}\n"
                      f"message key: {message.key()} || message value: {message.value()}")
                consumer.commit()
            else:
                print("No new messages at this point. Try again later.")
=======
        consumer_props = {'bootstrap.servers': props['bootstrap.servers'],
                          'group.id': 'datatalkclubs.taxirides.avro.consumer.2',
                          'auto.offset.reset': "earliest"}
        self.consumer = Consumer(consumer_props)
<<<<<<< HEAD
        self.consumer.subscribe(props['topics'])
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
=======
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))

    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                key = self.avro_key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                record = self.avro_value_deserializer(msg.value(),
                                                      SerializationContext(msg.topic(), MessageField.VALUE))
                if record is not None:
                    print("{}, {}".format(key, record))
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
<<<<<<< HEAD
    read_messages()

=======
        consumer_props = {'bootstrap.servers': props['bootstrap.servers'],
                          'group.id': 'datatalkclubs.taxirides.avro.consumer.2',
                          'auto.offset.reset': "earliest"}
        self.consumer = Consumer(consumer_props)

    @staticmethod
    def load_schema(schema_path: str):
        path = os.path.realpath(os.path.dirname(__file__))
        with open(f"{path}/{schema_path}") as f:
            schema_str = f.read()
        return schema_str

    def consume_from_kafka(self, topics: List[str]):
        self.consumer.subscribe(topics=topics)
        while True:
            try:
                # SIGINT can't be handled when polling, limit timeout to 1 second.
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                key = self.avro_key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
                record = self.avro_value_deserializer(msg.value(),
                                                      SerializationContext(msg.topic(), MessageField.VALUE))
                if record is not None:
                    print("{}, {}".format(key, record))
            except KeyboardInterrupt:
                break

        self.consumer.close()


if __name__ == "__main__":
=======
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
    config = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'schema_registry.url': SCHEMA_REGISTRY_URL,
        'schema.key': RIDE_KEY_SCHEMA_PATH,
        'schema.value': RIDE_VALUE_SCHEMA_PATH,
<<<<<<< HEAD
<<<<<<< HEAD
    }
    avro_consumer = RideAvroConsumer(props=config)
    avro_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
=======
        'topics': [KAFKA_TOPIC]
    }
    avro_consumer = RideAvroConsumer(props=config)
    avro_consumer.consume_from_kafka()
>>>>>>> 31118075 (Initiate PySpark streaming and refactor existing python-kafka examples (#325))
=======
    }
    avro_consumer = RideAvroConsumer(props=config)
    avro_consumer.consume_from_kafka(topics=[KAFKA_TOPIC])
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
