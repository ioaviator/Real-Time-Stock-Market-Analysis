from kafka import KafkaProducer
import json

topic = "stock_analysis"

def kafka_producer() -> KafkaProducer:
    """
    Creates and returns a KafkaProducer instance.

    Set up a Kafka producer that connects to a Kafka cluster 
    at 'localhost:9094' and serializes the message values to JSON format.

    Returns:
        KafkaProducer: The Kafka producer instance.

    Example (Call the function):
        producer = kafka_producer()
        producer.send('stock_analysis', {'key': 'value'})
    """
    producer = KafkaProducer(
        bootstrap_servers='localhost:9094',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    return producer
