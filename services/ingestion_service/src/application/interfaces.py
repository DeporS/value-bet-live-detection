from typing import Protocol, List
from shared_lib.domain.events import MatchEvent, OddsEvent

class MatchDataProvider(Protocol):
    """Protocol for providing match data events."""
    
    async def fetch_latest_events(self, match_id: str) -> List[MatchEvent]:
        """Fetch the latest match events for a given match ID."""
        ...
    
    async def fetch_latest_odds(self, match_id: str) -> OddsEvent | None:
        """Fetch the latest odds for a given match ID."""
        ...

class MessagePublisher(Protocol):
    """Protocol for publishing messages to a message broker."""
    
    async def publish_match_events(self, topic: str, events: List[MatchEvent]) -> None:
        """Publish a list of match events to the specified topic."""
        ...
    
    async def publish_odds_event(self, topic: str, event: OddsEvent) -> None:
        """Publish an odds event to the specified topic."""
        ...