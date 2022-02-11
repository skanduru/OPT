import pytest

from producers.simulation import TimeSimulation




@pytest.fixture
def setup():
    TS = TimeSimulation()
    return TS

