# Import necessary libraries
import json
from typing import List
from loguru import logger
from websocket import create_connection
from pydantic import BaseModel
from datetime import datetime, timezone


class Trade(BaseModel):
    """
    Model representing a single trade data from Kraken Websocket API.
    """
    product_id: str
    quantity: float
    price: float
    timestamp_ms: int


class KrakenWebsocketAPI:
    """
    Class for handling connections and reading real-time trades from Kraken Websocket API.
    """

    # URL for Kraken Websocket API
    URL = "wss://ws.kraken.com/v2"

    def __init__(self, product_id: str) -> None:
        """
        Initializes the KrakenWebsocketAPI instance.

        Args:
            product_id (str): The product ID (symbol) for which to subscribe and receive trades.
        """
        self.product_id = product_id

        # Create a WebSocket connection to the Kraken API
        self._ws = create_connection(self.URL)
        logger.info("Connection established")

        # Subscribe to trade data for the specified product
        self._subscribe(product_id)

    def get_trades(self) -> List[Trade]:
        """
        Retrieves the latest batch of trades from the Kraken Websocket API.

        Returns:
            List[Trade]: A list of Trade objects containing the trade details.
        """
        # Receive a message from the WebSocket connection
        message = self._ws.recv()

        # Check if the message is a heartbeat
        if "heartbeat" in message:
            logger.debug("Heartbeat received")
            return []

        # Parse the received message as JSON
        message = json.loads(message)

        # Initialize an empty list to store trade data
        trades = []

        # Process each trade entry in the message
        for trade in message["data"]:
            trades.append(
                Trade(
                    product_id=trade["symbol"],
                    price=trade["price"],
                    quantity=trade["qty"],
                    timestamp_ms=self.to_ms(trade["timestamp"]),
                )
            )

        return trades

    def is_done(self) -> bool:
        """
        Checks if the WebSocket connection to Kraken API is closed.

        Returns:
            bool: False (indicating the connection is still active)
        """
        return False

    @staticmethod
    def to_ms(timestamp: str) -> int:
        """
        Converts an ISO 8601 timestamp to milliseconds since the epoch.

        Args:
            timestamp (str): The ISO 8601 formatted timestamp.

        Returns:
            int: The timestamp in milliseconds since the epoch.
        """
        timestamp = datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp() * 1000)

    def _subscribe(self, product_id: str) -> None:
        """
        Establishes a connection to the Kraken WebSocket API and subscribes to trades for the given 'product_id'.

        Args:
            product_id (str): The product ID (symbol) to subscribe to.
        """
        logger.info(f"Subscribing to trades for {product_id}")

        # Subscription message for the Kraken WebSocket API
        msg = {
            "method": "subscribe",
            "params": {"channel": "trade", "symbol": [product_id], "snapshot": False},
        }

        # Send the subscription message
        self._ws.send(json.dumps(msg))
        logger.info("Subscription request sent")

        # Receive initial subscription confirmation messages
        _ = self._ws.recv()
        _ = self._ws.recv()  # Additional recv to handle possible subscription response delays
