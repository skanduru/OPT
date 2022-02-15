"""Defines core consumer functionality"""
import logging

import confluent_kafka
from confluent_kafka import Consumer
from confluent_kafka.avro import AvroConsumer, CachedSchemaRegistryClient
from confluent_kafka.avro.serializer import SerializerError
from tornado import gen


logger = logging.getLogger(__name__)

KAFKA_URL="PLAINTEXT://localhost:9092"
CL_GROUP_ID="cta_consumer"
SCHEMA_REGISTRY_URL="http://localhost:8081"

class KafkaConsumer:
    """Defines the base kafka consumer class"""

    def __init__(
        self,
        topic_name_pattern,
        message_handler,
        is_avro=True,
        offset_earliest=False,
        sleep_secs=1.0,
        consume_timeout=0.1,
    ):
        """Creates a consumer object for asynchronous use"""
        self.topic_name_pattern = topic_name_pattern
        self.message_handler = message_handler
        self.sleep_secs = sleep_secs
        self.consume_timeout = consume_timeout
        self.offset_earliest = offset_earliest

        #
        # Broker properties for the client connection and schema-registry
        #
        self.broker_properties = {
            'bootstrap.servers': KAFKA_URL,
            "group.id" : CL_GROUP_ID,
            "auto.offset.reset" : "earliest" if offset_earliest else "latest"
        }

        # Create client based on options
        if is_avro is True:
            # Create a Cached Schema Registry client
            schema_registry = CachedSchemaRegistryClient({'url': self.broker_properties.get('SCHEMA_REGISTRY')})

            self.broker_properties["schema.registry.url"] = SCHEMA_REGISTRY_URL
            self.consumer = AvroConsumer(
                         config = self.broker_properties,
                         schema_registry = schema_registry,
                        )
        else:
            self.consumer = Consumer(self.broker_properties)
            pass

        #
        #  subscribe to the topics
        # how the `on_assign` callback should be invoked.
        self.consumer.subscribe( [self.topic_name_pattern],
               on_assign = self.on_assign,
             )

    def on_assign(self, consumer, partitions):
        """Callback for when topic assignment takes place"""
        for partition in partitions:
            for partition in partitions:
                if self.offset_earliest:
                    partition.offset = OFFSET_BEGINNING

        logger.info("partitions assigned for %s", self.topic_name_pattern)
        consumer.assign(partitions)

    async def consume(self):
        """Asynchronously consumes data from kafka topic"""
        while True:
            num_results = 1
            while num_results > 0:
                num_results = await self._consume()
            await gen.sleep(self.sleep_secs)

    async def _consume(self):
        """
           Polls for a message.
           Returns 1 if a message was received, 0 otherwise
           Handle error cases
        """
        ret = 0
        message = self.consumer.poll(1.0)
        if message is None:
            print("No message received by consumer")
        elif message.error() is not None:
            print(f"error from consumer {message.error()}")
        else:
            try:
                logger.debug("handover to caller")
                self.message_handler(message)
                ret = 1
            except:
                logger.info(f"Failed to unpack message {e}")
        return ret


    def close(self):
        """Cleans up any open kafka consumers"""
        self.consumer.close()
        logger.info("Closing the active consumer")
