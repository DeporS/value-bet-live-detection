import logging
from typing import List
from aiokafka import AIOKafkaProducer # type: ignore

from application.interfaces import MessagePublisher
from shared_lib.domain.events import MatchEvent, OddsEvent

logger = logging.getLogger(__name__)

class KafkaMessagePublisher(MessagePublisher):
    """
    Adapter for publishing messages to Kafka using aiokafka.
    """

    def __init__(self, bootstrap_servers: str): # bootstrap_servers is the address of Kafka cluster
        self.bootstrap_servers = bootstrap_servers
        self.producer: AIOKafkaProducer | None = None

    async def start(self) -> None:
        """Initialize the Kafka producer. Must be called before publishing messages."""
        self.producer = AIOKafkaProducer(bootstrap_servers=self.bootstrap_servers)
        await self.producer.start()
        logger.info("Kafka producer started.")
        
    async def stop(self) -> None:
        """Closes the connection to Kafka. Important to not leak resources."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped.")
    
    async def publish_match_events(self, topic: str, events: List[MatchEvent]) -> None:
        if not self.producer:
            raise RuntimeError("Kafka producer is not initialized. Call start() before publishing messages.")
        
        for event in events:
            payload_bytes = event.model_dump_json().encode('utf-8') # Serialize Pydantic model to JSON and encode to bytes

            # Key ensures messages with the same match_id go to the same partition, preserving order for that match
            key_bytes = event.match_id.encode('utf-8')

            await self.producer.send_and_wait(topic, value=payload_bytes, key=key_bytes)
            logger.debug(f"Published match event {event.event_id} to topic {topic}")
    
    async def publish_odds_event(self, topic: str, event: OddsEvent) -> None:
        if not self.producer:
            raise RuntimeError("Kafka producer is not initialized. Call start() before publishing messages.")
        
        payload_bytes = event.model_dump_json().encode('utf-8')
        key_bytes = event.match_id.encode('utf-8')

        await self.producer.send_and_wait(topic, value=payload_bytes, key=key_bytes)
        logger.debug(f"Published odds event {event.event_id} to topic {topic}")