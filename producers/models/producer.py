"""Producer base-class providing common utilites and functionality"""
import logging
import time
import asyncio
import re


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer, CachedSchemaRegistryClient

logger = logging.getLogger(__name__)

KAFKA_URL="PLAINTEXT://localhost:9092"
ZOOKEEPER_URL="localhost:2181"
SCHEMA_REGISTRY_URL="http://localhost:8081"

def topic_pattern_exists(meta, pattern):
    topics = set(t.topic for t in iter(meta.topics.values()))
    pat = re.compile(pattern)
    match = []
    for x in topics:
        m = pat.search(x)
        if m:
            match.append(m.group(0))
    return match

class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self._loop = asyncio.get_event_loop()
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        #
        # bootstrap, zookeeper and schema registry configs
        self.broker_properties = {
            'KAFKA': KAFKA_URL,                      # Kafka broker
            'ZOOKEEPER': ZOOKEEPER_URL,              # Zookeeper
            'SCHEMA_REGISTRY': SCHEMA_REGISTRY_URL,  # Schema-Registry Server
        }

        # Create a Cached Schema Registry client
        schema_registry = CachedSchemaRegistryClient({'url': self.broker_properties.get('SCHEMA_REGISTRY')})

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            # Add a topic if it is not already in the list
            create = self.create_topic([self.topic_name])
            if create[0]:
                Producer.existing_topics.add(self.topic_name)

        # Configure the AvroProducer
        self.producer = AvroProducer(
            {'bootstrap.servers': self.broker_properties.get('KAFKA')},
            schema_registry = schema_registry,
            default_key_schema = key_schema,
            default_value_schema = value_schema,
        )

    def produce(self, topic, key, value):
        result = self._loop.create_future()
        def ack(err, msg):
            if err:
                self._loop.call_soon_threadsafe(result.set_exception,
                    KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, msg)
        self.producer.produce(
            topic=self.topic_name,
            key = key, value = value, on_delivery = ack)
        return result

    def create_topic(self, topics):
        """Creates the producer topic if it does not already exist"""
        #
        # Create new topics
        new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1, \
                         config = {
                            "compression.type": "lz4"
                         }) \
                        for topic in topics]
        #
        if not hasattr(self, 'admin_client'):
            admin_client = AdminClient({'bootstrap.servers': \
                       self.broker_properties.get('KAFKA')})
            self.admin_client = admin_client

        # Make sure the topics does not exist
        topic_meta = admin_client.list_topics(timeout = 5)
        topic_pattern_exists(topic_meta, 'jsontest')
        dup = False
        all_topics = []
        for key,val in topic_meta.topics.items():
            all_topics.append(val.topic)
        create = [True] * len(new_topics)
        topics = []
        for index, new_topic in enumerate(new_topics):
            if new_topic.topic in all_topics:
                create[index] = False
            else:
                topics.append(new_topic)
        if topics:
            futures = admin_client.create_topics(topics)
            for topic, future in futures.items():
                try:
                    future.result()
                    logger.debug('{topic.topic} created')
                except:
                    logger.debug('failed to create {topic.topic}')
        return create


    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        #
        #
        # cleanup code for the Producer here
        #
        #
        # Delete all topics. For now!
        if hasattr(self, 'admin_client'):
            list_topics = self.admin_client.list_topics()
            if list_topics:
                self.admin_client.delete_topics(list_topics.topics.values())
        self.producer.flush(timeout = 10)
        self.producer.close()

        logger.info("producer close incomplete - skipping")

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))
