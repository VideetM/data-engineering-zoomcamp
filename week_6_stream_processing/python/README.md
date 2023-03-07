<<<<<<< HEAD
## Introduction to Kafka
[Slides](https://docs.google.com/presentation/d/1bCtdCba8v1HxJ_uMm9pwjRUC-NAMeB-6nOG2ng3KujA/edit?usp=sharing)

- [Video: Intro to Kafka](https://www.youtube.com/watch?v=P1u8x3ycqvg&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=57)
- [Video: Configuration Terms](https://www.youtube.com/watch?v=Erf1-d1nyMY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=58)
- [Video: Avro and schema registry](https://www.youtube.com/watch?v=bzAsVNE5vOo&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=59)

### Configuration
Please take a look at all configuration from kafka [here](https://docs.confluent.io/platform/current/installation/configuration/).

### Docker
#### Starting cluster
=======
### Stream-Processing with Python

In this document, you will be finding information about stream processing 
using different Python libraries (`kafka-python`,`confluent-kafka`,`pyspark`, `faust`).

This Python module can be seperated in following modules.

####  1. Docker
Docker module includes, Dockerfiles and docker-compose definitions 
to run Kafka and Spark in a docker container. Setting up required services is
the prerequsite step for running following modules.

#### 2. Kafka Producer - Consumer Examples
- [Json Producer-Consumer Example](json_example) using `kafka-python` library
- [Avro Producer-Consumer Example](avro_example) using `confluent-kafka` library

Both of these examples require, up-and running Kafka services, therefore please ensure
following steps under [docker-README](docker/README.md)

To run the producer-consumer examples in the respective example folder, run following commands
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))
```bash
# Start producer script
python3 producer.py
# Start consumer script
python3 consumer.py
```

<<<<<<< HEAD
### Command line for Kafka
#### Create topic
```bash
./bin/kafka-topics.sh --create --topic demo_1 --bootstrap-server localhost:9092 --partitions 2
```

## KStreams
* [Slides](https://docs.google.com/presentation/d/1fVi9sFa7fL2ZW3ynS5MAZm0bRSZ4jO10fymPmrfTUjE/edit?usp=sharing)
* [Concepts](https://docs.confluent.io/platform/current/streams/concepts.html)

- [Video: KStream basics](https://www.youtube.com/watch?v=uuASDjCtv58&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=60)
- [Video: KStream Join and windowing](https://www.youtube.com/watch?v=dTzsDM9myr8&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=61)
- [Video: KStream advance features](https://www.youtube.com/watch?v=d8M_-ZbhZls&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=62)

#### Python Faust
* [Faust Doc](https://faust.readthedocs.io/en/latest/index.html)
* [KStream vs Faust](https://faust.readthedocs.io/en/latest/playbooks/vskafka.html)

#### JVM library
* [Confluent Kafka Stream](https://kafka.apache.org/documentation/streams/)
* [Example](https://github.com/AnkushKhanna/kafka-helper/tree/master/src/main/scala/kafka/schematest)

## Kafka Connect and KSQL
- [Video: Kafka connect and KSQL](https://www.youtube.com/watch?v=OgPJiic6xjY&list=PL3MmuxUbc_hJed7dXYoJw8DoCuVHhGEQb&index=63)

## Kafka connect
* [Blog post](https://medium.com/analytics-vidhya/making-sense-of-stream-data-b74c1252a8f5)

## Homework
[Form](https://forms.gle/mSzfpPCXskWCabeu5)

The homework is mostly theoretical. In the last question you have to provide working code link, please keep in mind that this 
question is not scored. 
=======
>>>>>>> cbe18f2f (Refactor python streaming examples (#337))

Deadline: 14 March, 22:00 CET



