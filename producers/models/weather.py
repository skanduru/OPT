"""Methods pertaining to weather data"""
from enum import IntEnum
import json
import logging
from pathlib import Path
import random
import urllib.parse
from confluent_kafka import avro

import requests

from .producer import Producer


logger = logging.getLogger(__name__)


class Weather(Producer):
    """Defines a simulated weather model"""

    status = IntEnum(
        "status", "sunny partly_cloudy cloudy windy precipitation", start=0
    )

    rest_proxy_url = "http://localhost:8082"

    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_key.json")
    value_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/weather_value.json")
    fd = open(f"{Path(__file__).parents[0]}/schemas/weather_value.json", 'r')
    vschema = fd.read()
    fd.close()
    fd = open(f"{Path(__file__).parents[0]}/schemas/weather_key.json", 'r')
    kschema = fd.read()
    fd.close()

    winter_months = set((0, 1, 2, 3, 10, 11))
    summer_months = set((6, 7, 8))

    def __init__(self, month):
        #
        # Create a topic for weather report
        #
        super().__init__(
            topic_name = f"{Weather.value_schema.namespace}",
            key_schema=Weather.key_schema,
            value_schema=Weather.value_schema,
            num_partitions=1,
            num_replicas=1,
        )

        self.status = Weather.status.sunny
        self.temp = 70.0
        if month in Weather.winter_months:
            self.temp = 40.0
        elif month in Weather.summer_months:
            self.temp = 85.0

        if Weather.key_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_key.json") as f:
                Weather.key_schema = json.load(f)

        #
        # Weather schema define this value schema per `schemas/weather_value.json
        #
        if Weather.value_schema is None:
            with open(f"{Path(__file__).parents[0]}/schemas/weather_value.json") as f:
                Weather.value_schema = json.load(f)

    def _set_weather(self, month):
        """Returns the current weather"""
        mode = 0.0
        if month in Weather.winter_months:
            mode = -1.0
        elif month in Weather.summer_months:
            mode = 1.0
        self.temp += min(max(-20.0, random.triangular(-10.0, 10.0, mode)), 100.0)
        self.status = random.choice(list(Weather.status))

    def run(self, month):
        self._set_weather(month)
        data = {
            "key_schema": Weather.kschema,
            "value_schema": Weather.vschema,
            "records": [
                {
                  "key": {"timestamp": self.time_millis()},
                  "value": {
                    "temperature": self.temp,
                    "status": self.status.name
                  }
                }
            ]
        }
        resp = requests.post(
            f"{Weather.rest_proxy_url}/topics/{Weather.value_schema.namespace}",
            # Avro serialization
            headers={"Content-Type":"application/vnd.kafka.avro.v2+json"},
            data = json.dumps(data)
        )
        try:
            resp.raise_for_status()
        except:
            logger.info(f"failed to send data to REST procy {json.dumps(resp.json(), indent=2)}")
        logger.debug(
            "sent weather data to kafka, temp: %s, status: %s",
            self.temp,
            self.status.name,
        )
