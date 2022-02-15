import logging
import pytest
from producers.connector import configure_connector

logger = logging.getLogger(__name__)

ts = None
def test_station(setup):
    global ts
    ts = setup

def test_connector():
    global ts
    logger.info("Beginning simulation, press Ctrl+C to exit at any time")
    logger.info("loading kafka connect jdbc source connector")
    ts.run_connector()

def test_weather_events(setup):
    global ts
    logger.info("running Weather event simulation")
    ts.run()
