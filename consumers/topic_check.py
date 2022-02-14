from confluent_kafka.admin import AdminClient
import re


def topic_exists(topic):
    """Checks if the given topic exists in Kafka"""
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    topic_metadata = client.list_topics(timeout=5)
    return topic in set(t.topic for t in iter(topic_metadata.topics.values()))

def topic_pattern_exists(pattern):
    """Checks if the given topic exists in Kafka"""
    match = []
    client = AdminClient({"bootstrap.servers": "PLAINTEXT://localhost:9092"})
    pat = re.compile(pattern)
    topic_meta = client.list_topics(timeout=5)
    topics = set(t.topic for t in iter(topic_meta.topics.values()))
    for x in topics:
        m = pat.search(x)
        if m:
            match.append(m.group(0))
    return match
