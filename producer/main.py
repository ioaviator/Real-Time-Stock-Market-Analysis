from extract import connect_to_api, extract_json
from producer_setup import kafka_producer, topic
import time

def main() -> None:
    """
    Main function that coordinates the process of fetching stock data, processing it, and sending it to Kafka.

    Performs the following tasks:
    1. Fetches stock data using `connect_to_api()`.
    2. Extracts and formats the relevant data using `extract_json()`.
    3. Sends the processed stock data to a Kafka topic via `kafka_producer()`.
    4. Introduces a 2-second delay between sending each record to avoid overwhelming the Kafka broker.

    Example:
        main()
    """
    # Fetch stock data from the API
    response = connect_to_api()

    # Extract and format the relevant stock data
    data = extract_json(response)

    # Set up the Kafka producer
    producer = kafka_producer()

    # Send each stock record to the Kafka topic
    for stock in data:
        result = {
            'date': stock['date'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'low': stock['low'],
            'high': stock['high'],
            'close': stock['close']
        }

        producer.send(topic, result)
        print(f'Data sent to {topic} topic')

        # Sleep to avoid overloading the Kafka broker
        time.sleep(2)

    # Flush and close the producer after sending all data
    producer.flush()
    producer.close()

    return


if __name__ == '__main__':
    main()
