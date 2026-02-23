import aiohttp
import logging
import asyncio
from typing import List, Optional
from datetime import datetime, UTC

from application.interfaces import MatchDataProvider
from shared_lib.domain.events import MatchEvent, OddsEvent, MatchStatsSnapshot

logger = logging.getLogger(__name__)

class FlashscoreProvider(MatchDataProvider):
    """
    Web Sniffing Adapter for Flashscore.com (Endpoint: df_st - Detail Feed Statistics).
    """
    def __init__(self):
        # Base URL from curl command observed in browser
        self.base_url = "https://2.flashscore.ninja/2/x/feed"
        self.session: Optional[aiohttp.ClientSession] = None

    async def connect(self) -> None:
        """Initialize the aiohttp session with headers rotating to mimic browser behavior."""
        headers = {
            "sec-ch-ua-platform": '"Windows"',
            "Referer": "https://www.flashscore.com/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36 Edg/145.0.0.0",
            "sec-ch-ua": '"Not:A-Brand";v="99", "Microsoft Edge";v="145", "Chromium";v="145"',
            "sec-ch-ua-mobile": "?0",
            "x-fsign": "SW9D1eZo"  # FLASHSCORE specific header, observed in browser requests (MAY CHANGE, need to monitor)
        }
        timeout = aiohttp.ClientTimeout(total=3.0)
        self.session = aiohttp.ClientSession(headers=headers, timeout=timeout)
        logger.info("Connected to Flashscore with custom headers.")

    async def disconnect(self) -> None:
        """Close the aiohttp session."""
        if self.session:
            await self.session.close()
            logger.info("Disconnected from Flashscore.")
    
    async def _fetch_text(self, url: str) -> str:
        """Safely fetch text from a URL."""
        try:
            async with self.session.get(url) as response:
                response.raise_for_status()
                return await response.text()
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return ""
    
    async def fetch_latest_events(self,match_id: str) -> List[MatchEvent]:
        if not self.session:
            raise RuntimeError("Session not initialized. Call connect() before fetching events.")
        
        # Construct the URL (df_st_1_ + Match ID) or (dc_1_ + Match ID) 
        url_stats = f"{self.base_url}/df_st_1_{match_id}"
        url_core = f"{self.base_url}/dc_1_{match_id}"

        try:
            stats_text, core_text = await asyncio.gather(
                self._fetch_text(url_stats),
                self._fetch_text(url_core)
            )
            
            return self._parse_flashscore_format(stats_text, core_text, match_id)

        except asyncio.TimeoutError:
            logger.warning(f"Timeout while fetching {match_id}")
            return []
        except aiohttp.ClientResponseError as e:
            # If 401/403 received - likely due to missing/invalid x-fsign or IP ban - log and return empty to avoid spamming
            logger.error(f"HTTP error (possible ban or invalid x-fsign): {e.status}")
            return []
        except Exception as e:
            logger.error(f"Unexpected integration error: {e}")
            return []
        
    def _parse_flashscore_format(self, raw_text: str, core_text: str, match_id: str) -> List[MatchEvent]:
        if not raw_text:
            return []
        
        # --- Core parsing (Goals) ---
        core_dict = {k: v for k, v in (pair.split("÷", 1) for pair in core_text.split("¬") if "÷" in pair)}
        
        home_goals = 0
        away_goals = 0

        try:
            home_goals = int(core_dict.get("DE", "0"))
            away_goals = int(core_dict.get("DF", "0"))
        except ValueError:
            logger.warning("Could not parse goals from Core Feed.")

        # --- Stats parsing ---
        rows = raw_text.split("~")
        is_match_section = False
        stats_dict = {}

        for row in rows:
            if not row.strip():
                continue
                
            row_data = {k: v for k, v in (pair.split("÷", 1) for pair in row.split("¬") if "÷" in pair)}
                    
            if row_data.get("SE") == "Match":
                is_match_section = True
            elif row_data.get("SE") in ["1st Half", "2nd Half"]:
                is_match_section = False
                
            if is_match_section and "SG" in row_data:
                stat_name = row_data["SG"]
                home_val = row_data.get("SH", "0").strip()
                away_val = row_data.get("SI", "0").strip()
                stats_dict[stat_name] = {"home": home_val, "away": away_val}

        if not stats_dict:
            return []

        # --- Helper functions ---
        
        def get_number(name: str, team: str, expected_type: type = int):
            """Fetches clean int and float values from stats_dict."""
            try:
                val = stats_dict.get(name, {}).get(team, "0")
                # Safety check to remove any non-numeric characters
                clean_val = val.replace("%", "").strip()
                return expected_type(clean_val)
            except (ValueError, TypeError):
                return expected_type(0)

        def get_percentage(name: str, team: str) -> float:
            """Extracts percentage values, converts to float between 0.0 and 1.0."""
            try:
                val = stats_dict.get(name, {}).get(team, "0")
                if "%" in val:
                    # Fetch everything before %
                    pct_str = val.split("%")[0].strip()
                    return float(pct_str) / 100.0
                return float(val) / 100.0
            except (ValueError, TypeError, IndexError):
                return 0.0

        # --- Create safe object ---
        snapshot = MatchStatsSnapshot(
            event_id=f"snap_{match_id}_{datetime.now().timestamp()}",
            match_id=match_id,
            timestamp=datetime.now(UTC),
            
            # Possession & Goals
            home_goals=home_goals,
            away_goals=away_goals,

            home_xg=get_number("Expected goals (xG)", "home", float),
            away_xg=get_number("Expected goals (xG)", "away", float),
            home_possession=get_percentage("Ball possession", "home"),
            away_possession=get_percentage("Ball possession", "away"),
            
            # Offense (Shots)
            home_total_shots=get_number("Total shots", "home", int),
            away_total_shots=get_number("Total shots", "away", int),
            home_shots_on_target=get_number("Shots on target", "home", int),
            away_shots_on_target=get_number("Shots on target", "away", int),
            home_shots_off_target=get_number("Shots off target", "home", int),
            away_shots_off_target=get_number("Shots off target", "away", int),
            home_shots_inside_box=get_number("Shots inside the box", "home", int),
            away_shots_inside_box=get_number("Shots inside the box", "away", int),
            home_shots_outside_box=get_number("Shots outside the box", "home", int),
            away_shots_outside_box=get_number("Shots outside the box", "away", int),
            home_big_chances=get_number("Big chances", "home", int),
            away_big_chances=get_number("Big chances", "away", int),
            
            # Creation & Playmaking
            home_corner_kicks=get_number("Corner kicks", "home", int),
            away_corner_kicks=get_number("Corner kicks", "away", int),
            home_offsides=get_number("Offsides", "home", int),
            away_offsides=get_number("Offsides", "away", int),
            home_free_kicks=get_number("Free kicks", "home", int),
            away_free_kicks=get_number("Free kicks", "away", int),
            home_crosses=get_number("Crosses", "home", int),
            away_crosses=get_number("Crosses", "away", int),
            
            # Passing effectiveness
            home_passes_pct=get_percentage("Passes", "home"),
            away_passes_pct=get_percentage("Passes", "away"),
            home_long_passes_pct=get_percentage("Long passes", "home"),
            away_long_passes_pct=get_percentage("Long passes", "away"),
            home_passes_final_third_pct=get_percentage("Passes in final third", "home"),
            away_passes_final_third_pct=get_percentage("Passes in final third", "away"),
            
            # Defense and Discipline
            home_fouls=get_number("Fouls", "home", int),
            away_fouls=get_number("Fouls", "away", int),
            home_tackles_pct=get_percentage("Tackles", "home"),
            away_tackles_pct=get_percentage("Tackles", "away"),
            home_duels_won=get_number("Duels won", "home", int),
            away_duels_won=get_number("Duels won", "away", int),
            home_clearances=get_number("Clearances", "home", int),
            away_clearances=get_number("Clearances", "away", int),
            home_interceptions=get_number("Interceptions", "home", int),
            away_interceptions=get_number("Interceptions", "away", int),
            home_yellow_cards=get_number("Yellow cards", "home", int),
            away_yellow_cards=get_number("Yellow cards", "away", int),
            home_red_cards=get_number("Red cards", "home", int),
            away_red_cards=get_number("Red cards", "away", int),
            
            # Goalkeeping
            home_goalkeeper_saves=get_number("Goalkeeper saves", "home", int),
            away_goalkeeper_saves=get_number("Goalkeeper saves", "away", int),
            home_xgot_faced=get_number("xGOT faced", "home", float),
            away_xgot_faced=get_number("xGOT faced", "away", float),
            home_goals_prevented=get_number("Goals prevented", "home", float),
            away_goals_prevented=get_number("Goals prevented", "away", float)
        )

        return [snapshot]

    async def fetch_latest_odds():
        # Maybe implement later
        return None
