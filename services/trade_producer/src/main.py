# Import necessary libraries and modules
from quixstreams import Application
from src.kraken_websocket_api import KrakenWebsocketAPI, Trade
from typing import List
from loguru import logger
from time import sleep


def produce_trades(kafka_broker_address: str, kafka_topic: str, product_id: str) -> None:
    """
    Reads trades from the Kraken WebSocket API and saves them to the specified Kafka topic.

    Args:
        kafka_broker_address (str): Address of the Kafka broker to connect to.
        kafka_topic (str): The Kafka topic where trades will be published.
        product_id (str): The product ID (symbol) for which to fetch trade data.

    Returns:
        None
    """
    # Initialize the Kafka application
    app = Application(broker_address=kafka_broker_address)

    # Create a topic object for the given Kafka topic with JSON serialization
    topic = app.topic(name=kafka_topic, value_serializer="json")

    # Create a Kraken API object to fetch trade data
    kraken_api = KrakenWebsocketAPI(product_id=product_id)

    # Open a Kafka producer context
    with app.get_producer() as producer:
        while True:
            # Fetch the latest batch of trades from the Kraken API
            trades: List[Trade] = kraken_api.get_trades()

            # Iterate over each trade and publish it to Kafka
            for trade in trades:
                # Serialize the trade data to a Kafka message
                message = topic.serialize(key=trade.product_id, value=trade.model_dump())

                # Produce the message to the specified Kafka topic
                producer.produce(topic=topic.name, value=message.value, key=message.key)

                # Log the action
                logger.debug(f"Pushed trade to Kafka: {trade}")

            # Sleep for a short period to avoid excessive API calls
            sleep(1)


if __name__ == "__main__":
    # Run the produce_trades function with predefined parameters
    produce_trades(
        kafka_broker_address="localhost:19092",
        kafka_topic="trades",
        product_id="ETH/USD",
    )
