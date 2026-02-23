import aiohttp
import logging
import asyncio
from typing import List, Optional
from datetime import datetime, UTC

from application.interfaces import MatchDataProvider
from shared_lib.domain.events import MatchEvent, OddsEvent

logger = logging.getLogger(__name__)

class FlashscoreProvider(MatchDataProvider):
    """
    Web Sniffing Adapter for Flashscore.com (Endpoint: df_st - Detail Feed Statistics).
    """
    def __init__(self):
        # Base URL from curl command observed in browser
        self.base_url = "https://2.flashscore.ninja/2/x/feed"
        self.session: Optional[aiohttp.ClientSession] = None
        self.processed_event_ids: set[str] = set()

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
    
    async def fetch_latest_events(self,match_id: str) -> List[MatchEvent]:
        if not self.session:
            raise RuntimeError("Session not initialized. Call connect() before fetching events.")
        
        # Construct the URL (df_st_1_ + Match ID)
        endpoint = f"{self.base_url}/df_st_1_{match_id}"

        try:
            async with self.session.get(endpoint) as response:
                response.raise_for_status()  # Raise error for bad status codes

                raw_text = await response.text()

                return self._parse_flashscore_format(raw_text, match_id)

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
        
    def _parse_flashscore_format(self, raw_text: str, match_id: str) -> List[MatchEvent]:
        """
        Defensive parsing of Flashscore custom format. 
        """
        new_events = []

        logger.debug(f"Raw response for {match_id}: {raw_text[:200]}...")  # Log the beginning of the response for debugging

        # Flashscore uses "~" as a delimiter
        rows = raw_text.split("~")

        for row in rows:
            if not row.strip():
                continue
            
            # TODO: The actual parsing logic will depend on the observed structure of the response.

            pass
        
        return new_events

    async def fetch_latest_odds():
        # Maybe implement later
        return None
