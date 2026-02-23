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

class MatchStatsSnapshot(BaseEvent):
    """Full snapshot of match statistics at a given point in time. Useful for training data and model features."""
    event_type: Literal["stats_snapshot"] = "stats_snapshot"

    # --- Time ---
    minute: int = Field(default=0, ge=0, le=150)
    
    # --- Possession & Goals ---
    home_goals: int = Field(default=0, ge=0)
    away_goals: int = Field(default=0, ge=0)
    home_xg: float = Field(default=0.0, ge=0.0)
    away_xg: float = Field(default=0.0, ge=0.0)
    home_possession: float = Field(default=0.0, ge=0.0, le=1.0)
    away_possession: float = Field(default=0.0, ge=0.0, le=1.0)
    
    # --- Attack (Shots) ---
    home_total_shots: int = Field(default=0, ge=0)
    away_total_shots: int = Field(default=0, ge=0)
    home_shots_on_target: int = Field(default=0, ge=0)
    away_shots_on_target: int = Field(default=0, ge=0)
    home_shots_off_target: int = Field(default=0, ge=0)
    away_shots_off_target: int = Field(default=0, ge=0)
    home_shots_inside_box: int = Field(default=0, ge=0)
    away_shots_inside_box: int = Field(default=0, ge=0)
    home_shots_outside_box: int = Field(default=0, ge=0)
    away_shots_outside_box: int = Field(default=0, ge=0)
    home_big_chances: int = Field(default=0, ge=0)
    away_big_chances: int = Field(default=0, ge=0)
    
    # --- Creation & Playmaking ---
    home_corner_kicks: int = Field(default=0, ge=0)
    away_corner_kicks: int = Field(default=0, ge=0)
    home_offsides: int = Field(default=0, ge=0)
    away_offsides: int = Field(default=0, ge=0)
    home_free_kicks: int = Field(default=0, ge=0)
    away_free_kicks: int = Field(default=0, ge=0)
    home_crosses: int = Field(default=0, ge=0)
    away_crosses: int = Field(default=0, ge=0)
    
    # Percentage effectiveness (0.0 - 1.0)
    home_passes_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    away_passes_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    home_long_passes_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    away_long_passes_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    home_passes_final_third_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    away_passes_final_third_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    
    # --- Defense ---
    home_fouls: int = Field(default=0, ge=0)
    away_fouls: int = Field(default=0, ge=0)
    home_tackles_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    away_tackles_pct: float = Field(default=0.0, ge=0.0, le=1.0)
    home_duels_won: int = Field(default=0, ge=0)
    away_duels_won: int = Field(default=0, ge=0)
    home_clearances: int = Field(default=0, ge=0)
    away_clearances: int = Field(default=0, ge=0)
    home_interceptions: int = Field(default=0, ge=0)
    away_interceptions: int = Field(default=0, ge=0)
    home_yellow_cards: int = Field(default=0, ge=0)
    away_yellow_cards: int = Field(default=0, ge=0)
    home_red_cards: int = Field(default=0, ge=0)
    away_red_cards: int = Field(default=0, ge=0)
    
    # --- Goalkeeping ---
    home_goalkeeper_saves: int = Field(default=0, ge=0)
    away_goalkeeper_saves: int = Field(default=0, ge=0)
    home_xgot_faced: float = Field(default=0.0, ge=0.0)
    away_xgot_faced: float = Field(default=0.0, ge=0.0)
    # Difference between xG faced and actual goals conceded
    home_goals_prevented: float = Field(default=0.0)
    away_goals_prevented: float = Field(default=0.0)