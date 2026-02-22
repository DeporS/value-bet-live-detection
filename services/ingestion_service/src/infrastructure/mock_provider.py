import asyncio
import random
from typing import List
from datetime import datetime, UTC
from application.interfaces import MatchDataProvider
from shared_lib.domain.events import MatchEvent, OddsEvent

class MockMatchProvider(MatchDataProvider):
    """
    Simulator of external API (e.g., Opta).
    Implements methods from MatchDataProvider - Application layer wont know the difference between this and a real provider.
    """

    async def fetch_latest_events(self, match_id: str) -> List[MatchEvent]:
        # Simulate network delay (latency)
        await asyncio.sleep(random.uniform(0.1, 0.5)) # asyncio.sleep to not block the event loop

        # Simulate that 80% of examples returns empty list (nothing happenes) to not spam kafka each second
        if rand.random() > 0.2:
            return []
        
        # If something happens - select random event
        event_types = ["shot", "goal", "corner", "card", "possession_update"]
        chosen_type = random.choice(event_types)

        # Create Pydantic object with random data - this will be validated by Pydantic and ensure correct structure
        event = MatchEvent(
            event_id=f"evt_{random.randint(1000, 9999)}",
            match_id=match_id,
            timestamp=datetime.now(UTC),
            event_type=chosen_type,
            minute=random.randint(1, 90),
            team_id=random.choice(["team_A", "team_B"]),
            xg_value=random.uniform(0.01, 0.99) if chosen_type in ["shot", "goal"] else None
        )
        return [event]
    
    async def fetch_latest_odds(self, match_id: str) -> OddsEvent | None:
        # Simulate network delay (latency)
        await asyncio.sleep(random.uniform(0.1, 0.3)) 

        if random.random() > 0.1: # odds update happens less frequently than match events
            return None
        
        # Generate random odds
        return OddsEvent(
            event_id=f"odds_{random.randint(1000, 9999)}",
            match_id=match_id,
            timestamp=datetime.now(UTC),
            home_odds=random.uniform(1.5, 3.0),
            draw_odds=random.uniform(2.5, 4.0),
            away_odds=random.uniform(2.0, 5.0)
        )