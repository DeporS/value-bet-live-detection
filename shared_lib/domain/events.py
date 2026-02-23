from datetime import datetime
from typing import Literal, Optional
from pydantic import BaseModel, Field, ConfigDict

class BaseEvent(BaseModel):
    """Base class for all events in the system."""
    model_config = ConfigDict(frozen=True) # Make the model immutable

    event_id: str = Field(..., description="Unique identifier for the event")
    match_id: str = Field(..., description="Identifier for the match")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC), description="Timestamp of the event") # default_factory and lambda to ensure dynamic default value at runtime

class MatchEvent(BaseEvent):
    """Match events (e.g., goal, strike, corner, etc.)"""
    event_type: Literal["shot", "goal", "corner", "card", "possession_update"]
    minute: int = Field(..., ge=0, le=130, description="Minute of the match")
    team_id: Optional[str] = Field(default=None, description="ID of team if applicable")
    xg_value: Optional[float] = Field(default=None, ge=0.0, le=1.0, description="Expected goals value")

class OddsEvent(BaseEvent):
    """Odds update 1X2"""
    home_odds: float = Field(..., gt=1.0)
    draw_odds: float = Field(..., gt=1.0)
    away_odds: float = Field(..., gt=1.0)

    @property
    def implied_probabilities(self) -> dict[str, float]:
        """Calculate implied probability including bookmaker margin."""
        home_p = 1 / self.home_odds
        draw_p = 1 / self.draw_odds
        away_p = 1 / self.away_odds

        # Calculate total implied probability (including margin)
        total_p = home_p + draw_p + away_p

        return {
            "home": home_p / total_p,
            "draw": draw_p / total_p,
            "away": away_p / total_p
        }

class ValueBetEvent(BaseEvent):
    """Alert for potential value bet opportunity"""
    team_id: str
    model_probability: float = Field(..., ge=0.0, le=1.0)
    implied_probability: float = Field(..., ge=0.0, le=1.0)
    expected_value: float = Field(..., description="Expected value of the bet (model_probability - implied_probability)")
    current_odds: float = Field(..., gt=1.0)