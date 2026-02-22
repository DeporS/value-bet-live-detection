import asyncio
import logging
from application.interfaces import MatchDataProvider, MessagePublisher

logger = logging.getLogger(__name__)

class IngestionOrchestrator:
    """Orchestrator for ingesting match data and publishing events."""
    
    def __init__(
        self,
        provider: MatchDataProvider,
        publisher: MessagePublisher,
        match_events_topic: str = "raw_match_events",
        odds_topic: str = "raw_odds_events"
    ):
        self.provider = provider
        self.publisher = publisher
        self.match_events_topic = match_events_topic
        self.odds_topic = odds_topic
    
    async def run_ingestion_loop(self, match_id: str, interval_seconds: int = 5) -> None:
        """Continuously fetch and publish match data at specified intervals."""
        
        logger.info(f"Starting ingestion loop for match_id={match_id} with interval={interval_seconds}s")

        try:
            while True:
                # Parallel fetching of match events and odds
                events_task = self.provider.fetch_latest_events(match_id)
                odds_task = self.provider.fetch_latest_odds(match_id)

                events, odds = await asyncio.gather(events_task, odds_task, return_exceptions=True)

                # Error handling for single shot - wont break the loop
                if isinstance(events, Exception):
                    logger.error(f"Error fetching events: {events}")
                elif events:
                    await self.publisher.publish_match_events(self.match_events_topic, events)
                    logger.info(f"Published {len(events)} match events for match_id={match_id}")
                
                if isinstance(odds, Exception):
                    logger.error(f"Error fetching odds: {odds}")
                elif odds:
                    await self.publisher.publish_odds_event(self.odds_topic, odds)
                    logger.info(f"Published odds event for match_id={match_id}")
                
                # Wait for the next interval
                await asyncio.sleep(interval_seconds)
        
        except asyncio.CancelledError:
            logger.info(f"Stopped ingestion loop for match_id={match_id}.")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in ingestion loop: {e}", exc_info=True)
            raise