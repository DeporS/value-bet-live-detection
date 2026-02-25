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
    
    async def run_ingestion_loop(self, match_id: str, interval_seconds: int = 5, stop_event: asyncio.Event = None) -> None:
        """Continuously fetch and publish match data at specified intervals."""
        
        logger.info(f"Starting ingestion loop for match_id={match_id} with interval={interval_seconds}s")

        halftime_sleep_done = False # Flag to track if halftime sleep adjustment has been applied

        try:
            while not (stop_event and stop_event.is_set()):

                # Parallel fetching of match events and odds
                events_task = self.provider.fetch_latest_events(match_id)
                odds_task = self.provider.fetch_latest_odds(match_id)

                events, odds = await asyncio.gather(events_task, odds_task, return_exceptions=True)

                # Error handling for single shot - wont break the loop
                if isinstance(events, Exception):
                    logger.error(f"Error fetching events: {events}")
                    
                elif events:
                    # Send to Kafka
                    await self.publisher.publish_match_events(self.match_events_topic, events)
                    logger.info(f"Published {len(events)} match events for match_id={match_id}")
                
                if isinstance(odds, Exception):
                    logger.error(f"Error fetching odds: {odds}")
                elif odds:
                    await self.publisher.publish_odds_event(self.odds_topic, odds)
                    logger.info(f"Published odds event for match_id={match_id}")
                
                # --- INTELLIGENT SLEEP & SHUTDOWN LOGIC ---
                current_sleep_time = interval_seconds
                status = getattr(self.provider, "current_match_status", 0)

                # Status 3: Match finished
                if status == 3:
                    logger.info(f"Match {match_id} finished. Shutting down ingestion loop.")
                    if stop_event:
                        stop_event.set() # Signal the loop to stop
                    break # Exit the loop immediately
                
                # Status 38: Halftime break
                elif status == 38 and not halftime_sleep_done:
                    logger.info(f"Match {match_id} at halftime. Increasing sleep interval to 840 seconds.")
                    current_sleep_time = 840 # Longer sleep during halftime
                    halftime_sleep_done = True # Ensure we only apply this adjustment once

                
                # Clever waiting mechanism that allows for graceful shutdown without blocking the event loop
                if stop_event:
                    try:
                        await asyncio.wait_for(stop_event.wait(), timeout=current_sleep_time)
                    except asyncio.TimeoutError:
                        pass
                else:
                    # Fallback if no stop_event provided - just sleep for the interval
                    await asyncio.sleep(current_sleep_time)
        
        except asyncio.CancelledError:
            logger.info(f"Stopped ingestion loop for match_id={match_id}.")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in ingestion loop: {e}", exc_info=True)
            raise