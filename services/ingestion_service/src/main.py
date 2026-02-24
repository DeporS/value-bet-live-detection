import asyncio
import logging
import signal
import sys
import os

from application.ingestion_app import IngestionOrchestrator
from infrastructure.mock_provider import MockMatchProvider
from infrastructure.flashscore_provider import FlashscoreProvider
from infrastructure.kafka_publisher import KafkaMessagePublisher

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger("IngestionService")

async def main() -> None:
    logger.info("Starting Ingestion Service...")

    # Environment variables
    KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092") # Address of Kafka cluster
    MATCH_ID = os.getenv("MATCH_ID", "KC9vVKct") # For testing set to a fixed match ID

    # Initialize provider and publisher (Adapters)
    provider = FlashscoreProvider()
    publisher = KafkaMessagePublisher(bootstrap_servers=KAFKA_BROKER)

    # Composition of the application
    orchestrator = IngestionOrchestrator(
        provider=provider,
        publisher=publisher,
        match_events_topic="raw_match_events",
        odds_topic="raw_odds_events"
    )

    # Launch network resources (Kafka producer)
    await publisher.start()
    await provider.connect()

    # Graceful shutdown handling
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def handle_shutdown_signal():
        logger.warning("Shutdown signal received. Stopping graceful ingestion loop...")
        stop_event.set()
    
    # Register listening for termination signals (SIGINT for Ctrl+C, SIGTERM for container shutdown)
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, handle_shutdown_signal)
        except NotImplementedError:
            pass # Signal handlers may not be implemented on some platforms (e.g., Windows)

    # Run the ingestion loop in the background
    ingestion_task = asyncio.create_task(
        orchestrator.run_ingestion_loop(match_id=MATCH_ID, interval_seconds=5, stop_event=stop_event)
    )

    # Wait until a shutdown signal is received
    await stop_event.wait() # This will block until stop_event.set() is called in the signal handler

    # Cleanup
    logger.info("Shutting down Ingestion Service...")
    ingestion_task.cancel() # Cancel the ingestion loop task

    # Wait for the ingestion loop to finish cleanup
    try:
        await ingestion_task
    except asyncio.CancelledError:
        pass
    
    # Safely close network resources
    await publisher.stop()
    logger.info("Ingestion Service stopped gracefully.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # Another layer of safety for graceful shutdown if signal handling is not supported
        logger.info("Ingestion Service interrupted by user. Exiting...")
        pass     