import pytest
import asyncio
import aiohttp
from datetime import datetime, UTC
from unittest.mock import patch, MagicMock
from services.ingestion_service.src.infrastructure.flashscore_provider import FlashscoreProvider

@pytest.fixture
def provider() -> FlashscoreProvider:
    """Returns a clean instance of FlashscoreProvider."""
    return FlashscoreProvider(proxy_url=None) # Use None to test default proxy behavior

def test_parse_flashscore_extract_goals_and_possession(provider: FlashscoreProvider):
    """
    Ensures the parser correctly extracts score and possession from raw Flashscore strings.
    We do not make any HTTP requests here; we feed it static mock data.
    """

    mock_match_id = "KbUrxW1T"

    mock_core_text = "DA÷3¬DZ÷3¬DB÷3¬DD÷1772297840¬AW÷1¬DC÷1772290800¬DS÷0¬DE÷5¬DF÷2¬DG÷5¬DH÷2¬DI÷-1¬DK÷1772297844¬DL÷1¬DM÷¬DX÷MR,ST,PS,LI,PMS,LC,MC,OD,HH,TTS,SCR,LT,TA,NF,HITO,HIO¬DEI÷https://static.flashscore.com/res/image/data/Gr0cGteM-OOwF2iN9-Q9DJHs4l.png¬DV÷1¬DT÷¬DJ÷H¬AZ÷1¬QQ÷https://api.lsaudio.eu/getStream/match-en-ApYQgDd1pb?fs_project=2¬QJ÷https://api.lsaudio.eu/getStreamV2/match-en-ApYQgDd1pb?fs_project=2¬QX÷Matt Roberts¬SC÷165¬SB÷1¬SD÷STS.pl¬A1÷¬~"

    mock_stats_text="SE÷Match¬~SF÷Top stats¬~SD÷432¬SG÷Expected goals (xG)¬SH÷1.84¬SI÷1.85¬~SD÷12¬SG÷Ball possession¬SH÷49%¬SI÷51%¬~SD÷34¬SG÷Total shots¬SH÷18¬SI÷11¬~SD÷13¬SG÷Shots on target¬SH÷7¬SI÷4¬~SD÷459¬SG÷Big chances¬SH÷2¬SI÷2¬~SD÷16¬SG÷Corner kicks¬SH÷10¬SI÷5¬~SD÷342¬SG÷Passes¬SH÷81% (328/405)¬SI÷79% (332/422)¬~SD÷23¬SG÷Yellow cards¬SH÷2¬SI÷2¬~SF÷Shots¬~SD÷432¬SG÷Expected goals (xG)¬SH÷1.84¬SI÷1.85¬~SD÷499¬SG÷xG on target (xGOT)¬SH÷2.44¬SI÷1.95¬~SD÷34¬SG÷Total shots¬SH÷18¬SI÷11¬~SD÷13¬SG÷Shots on target¬SH÷7¬SI÷4¬~SD÷14¬SG÷Shots off target¬SH÷4¬SI÷4¬~SD÷158¬SG÷Blocked shots¬SH÷7¬SI÷3¬~SD÷461¬SG÷Shots inside the box¬SH÷13¬SI÷8¬~SD÷463¬SG÷Shots outside the box¬SH÷5¬SI÷3¬~SD÷457¬SG÷Hit the woodwork¬SH÷0¬SI÷0¬~SD÷465¬SG÷Headed goals¬SH÷1¬SI÷1¬~SF÷Attack¬~SD÷459¬SG÷Big chances¬SH÷2¬SI÷2¬~SD÷16¬SG÷Corner kicks¬SH÷10¬SI÷5¬~SD÷471¬SG÷Touches in opposition box¬SH÷34¬SI÷31¬~SD÷521¬SG÷Accurate through passes¬SH÷0¬SI÷0¬~SD÷17¬SG÷Offsides¬SH÷1¬SI÷1¬~SD÷15¬SG÷Free kicks¬SH÷11¬SI÷12¬~SF÷Passes¬~SD÷342¬SG÷Passes¬SH÷81% (328/405)¬SI÷79% (332/422)¬~SD÷517¬SG÷Long passes¬SH÷44% (17/39)¬SI÷44% (25/57)¬~SD÷467¬SG÷Passes in final third¬SH÷74% (104/140)¬SI÷58% (67/116)¬~SD÷433¬SG÷Crosses¬SH÷10% (2/20)¬SI÷19% (4/21)¬~SD÷503¬SG÷Expected assists (xA)¬SH÷1.32¬SI÷0.81¬~SD÷18¬SG÷Throw ins¬SH÷20¬SI÷21¬~SF÷Defense¬~SD÷21¬SG÷Fouls¬SH÷12¬SI÷11¬~SD÷475¬SG÷Tackles¬SH÷48% (10/21)¬SI÷62% (8/13)¬~SD÷513¬SG÷Duels won¬SH÷46¬SI÷49¬~SD÷479¬SG÷Clearances¬SH÷22¬SI÷19¬~SD÷434¬SG÷Interceptions¬SH÷6¬SI÷13¬~SD÷507¬SG÷Errors leading to shot¬SH÷2¬SI÷0¬~SD÷509¬SG÷Errors leading to goal¬SH÷0¬SI÷0¬~SF÷Goalkeeping¬~SD÷19¬SG÷Goalkeeper saves¬SH÷3¬SI÷2¬~SD÷501¬SG÷xGOT faced¬SH÷1.95¬SI÷2.44¬~SD÷511¬SG÷Goals prevented¬SH÷-0.05¬SI÷-1.56¬~SE÷1st Half¬~SF÷Top stats¬~SD÷432¬SG÷Expected goals (xG)¬SH÷0.83¬SI÷0.58¬~SD÷12¬SG÷Ball possession¬SH÷51%¬SI÷49%¬~SD÷34¬SG÷Total shots¬SH÷10¬SI÷5¬~SD÷13¬SG÷Shots on target¬SH÷5¬SI÷0¬~SD÷459¬SG÷Big chances¬SH÷0¬SI÷0¬~SD÷16¬SG÷Corner kicks¬SH÷8¬SI÷2¬~SD÷342¬SG÷Passes¬SH÷82% (174/211)¬SI÷79% (166/210)¬~SD÷23¬SG÷Yellow cards¬SH÷0¬SI÷1¬~SF÷Shots¬~SD÷432¬SG÷Expected goals (xG)¬SH÷0.83¬SI÷0.58¬~SD÷499¬SG÷xG on target (xGOT)¬SH÷1.94¬SI÷0.00¬~SD÷34¬SG÷Total shots¬SH÷10¬SI÷5¬~SD÷13¬SG÷Shots on target¬SH÷5¬SI÷0¬~SD÷14¬SG÷Shots off target¬SH÷0¬SI÷2¬~SD÷158¬SG÷Blocked shots¬SH÷5¬SI÷3¬~SD÷461¬SG÷Shots inside the box¬SH÷7¬SI÷4¬~SD÷463¬SG÷Shots outside the box¬SH÷3¬SI÷1¬~SD÷457¬SG÷Hit the woodwork¬SH÷0¬SI÷0¬~SD÷465¬SG÷Headed goals¬SH÷1¬SI÷0¬~SF÷Attack¬~SD÷459¬SG÷Big chances¬SH÷0¬SI÷0¬~SD÷16¬SG÷Corner kicks¬SH÷8¬SI÷2¬~SD÷471¬SG÷Touches in opposition box¬SH÷19¬SI÷16¬~SD÷521¬SG÷Accurate through passes¬SH÷0¬SI÷0¬~SD÷17¬SG÷Offsides¬SH÷1¬SI÷0¬~SD÷15¬SG÷Free kicks¬SH÷5¬SI÷6¬~SF÷Passes¬~SD÷342¬SG÷Passes¬SH÷82% (174/211)¬SI÷79% (166/210)¬~SD÷517¬SG÷Long passes¬SH÷47% (8/17)¬SI÷48% (13/27)¬~SD÷467¬SG÷Passes in final third¬SH÷80% (55/69)¬SI÷58% (38/65)¬~SD÷433¬SG÷Crosses¬SH÷14% (2/14)¬SI÷9% (1/11)¬~SD÷503¬SG÷Expected assists (xA)¬SH÷0.93¬SI÷0.27¬~SD÷18¬SG÷Throw ins¬SH÷9¬SI÷10¬~SF÷Defense¬~SD÷21¬SG÷Fouls¬SH÷6¬SI÷5¬~SD÷475¬SG÷Tackles¬SH÷40% (2/5)¬SI÷56% (5/9)¬~SD÷513¬SG÷Duels won¬SH÷16¬SI÷27¬~SD÷479¬SG÷Clearances¬SH÷10¬SI÷16¬~SD÷434¬SG÷Interceptions¬SH÷3¬SI÷8¬~SD÷507¬SG÷Errors leading to shot¬SH÷1¬SI÷0¬~SD÷509¬SG÷Errors leading to goal¬SH÷0¬SI÷0¬~SF÷Goalkeeping¬~SD÷19¬SG÷Goalkeeper saves¬SH÷1¬SI÷1¬~SD÷501¬SG÷xGOT faced¬SH÷0.00¬SI÷1.94¬~SD÷511¬SG÷Goals prevented¬SH÷0.00¬SI÷-1.06¬~SE÷2nd Half¬~SF÷Top stats¬~SD÷432¬SG÷Expected goals (xG)¬SH÷1.01¬SI÷1.27¬~SD÷12¬SG÷Ball possession¬SH÷47%¬SI÷53%¬~SD÷34¬SG÷Total shots¬SH÷8¬SI÷6¬~SD÷13¬SG÷Shots on target¬SH÷2¬SI÷4¬~SD÷459¬SG÷Big chances¬SH÷2¬SI÷2¬~SD÷16¬SG÷Corner kicks¬SH÷2¬SI÷3¬~SD÷342¬SG÷Passes¬SH÷79% (154/194)¬SI÷78% (166/212)¬~SD÷23¬SG÷Yellow cards¬SH÷2¬SI÷1¬~SF÷Shots¬~SD÷432¬SG÷Expected goals (xG)¬SH÷1.01¬SI÷1.27¬~SD÷499¬SG÷xG on target (xGOT)¬SH÷0.50¬SI÷1.95¬~SD÷34¬SG÷Total shots¬SH÷8¬SI÷6¬~SD÷13¬SG÷Shots on target¬SH÷2¬SI÷4¬~SD÷14¬SG÷Shots off target¬SH÷4¬SI÷2¬~SD÷158¬SG÷Blocked shots¬SH÷2¬SI÷0¬~SD÷461¬SG÷Shots inside the box¬SH÷6¬SI÷4¬~SD÷463¬SG÷Shots outside the box¬SH÷2¬SI÷2¬~SD÷457¬SG÷Hit the woodwork¬SH÷0¬SI÷0¬~SD÷465¬SG÷Headed goals¬SH÷0¬SI÷1¬~SF÷Attack¬~SD÷459¬SG÷Big chances¬SH÷2¬SI÷2¬~SD÷16¬SG÷Corner kicks¬SH÷2¬SI÷3¬~SD÷471¬SG÷Touches in opposition box¬SH÷15¬SI÷15¬~SD÷521¬SG÷Accurate through passes¬SH÷0¬SI÷0¬~SD÷17¬SG÷Offsides¬SH÷0¬SI÷1¬~SD÷15¬SG÷Free kicks¬SH÷6¬SI÷6¬~SF÷Passes¬~SD÷342¬SG÷Passes¬SH÷79% (154/194)¬SI÷78% (166/212)¬~SD÷517¬SG÷Long passes¬SH÷41% (9/22)¬SI÷40% (12/30)¬~SD÷467¬SG÷Passes in final third¬SH÷69% (49/71)¬SI÷57% (29/51)¬~SD÷433¬SG÷Crosses¬SH÷0% (0/6)¬SI÷30% (3/10)¬~SD÷503¬SG÷Expected assists (xA)¬SH÷0.39¬SI÷0.54¬~SD÷18¬SG÷Throw ins¬SH÷11¬SI÷11¬~SF÷Defense¬~SD÷21¬SG÷Fouls¬SH÷6¬SI÷6¬~SD÷475¬SG÷Tackles¬SH÷50% (8/16)¬SI÷75% (3/4)¬~SD÷513¬SG÷Duels won¬SH÷30¬SI÷22¬~SD÷479¬SG÷Clearances¬SH÷12¬SI÷3¬~SD÷434¬SG÷Interceptions¬SH÷3¬SI÷5¬~SD÷507¬SG÷Errors leading to shot¬SH÷1¬SI÷0¬~SD÷509¬SG÷Errors leading to goal¬SH÷0¬SI÷0¬~SF÷Goalkeeping¬~SD÷19¬SG÷Goalkeeper saves¬SH÷2¬SI÷1¬~SD÷501¬SG÷xGOT faced¬SH÷1.95¬SI÷0.50¬~SD÷511¬SG÷Goals prevented¬SH÷-0.05¬SI÷-0.50¬~A1÷¬~"

    # Execute parsing
    result = provider._parse_flashscore_format(
        raw_text=mock_stats_text, 
        core_text=mock_core_text, 
        match_id=mock_match_id
    )

    # Assert response structure
    assert len(result) == 1, f"Expected exactly 1 snapshot, got {len(result)}"
    snapshot = result[0]

    expected_values = {
        "event_type": "stats_snapshot",
        "match_id": "KbUrxW1T",
        "minute": 90,
        "second": 0,
        "home_goals": 5,
        "away_goals": 2,
        "home_xg": 1.84,
        "away_xg": 1.85,
        "home_possession": 0.49,
        "away_possession": 0.51,
        "home_total_shots": 18,
        "away_total_shots": 11,
        "home_shots_on_target": 7,
        "away_shots_on_target": 4,
        "home_shots_off_target": 4,
        "away_shots_off_target": 4,
        "home_shots_inside_box": 13,
        "away_shots_inside_box": 8,
        "home_shots_outside_box": 5,
        "away_shots_outside_box": 3,
        "home_big_chances": 2,
        "away_big_chances": 2,
        "home_corner_kicks": 10,
        "away_corner_kicks": 5,
        "home_offsides": 1,
        "away_offsides": 1,
        "home_free_kicks": 11,
        "away_free_kicks": 12,
        "home_passes_pct": 0.81,
        "away_passes_pct": 0.79,
        "home_long_passes_pct": 0.44,
        "away_long_passes_pct": 0.44,
        "home_passes_final_third_pct": 0.74,
        "away_passes_final_third_pct": 0.58,
        "home_crosses_pct": 0.1,
        "away_crosses_pct": 0.19,
        "home_fouls": 12,
        "away_fouls": 11,
        "home_tackles_pct": 0.48,
        "away_tackles_pct": 0.62,
        "home_duels_won": 46,
        "away_duels_won": 49,
        "home_clearances": 22,
        "away_clearances": 19,
        "home_interceptions": 6,
        "away_interceptions": 13,
        "home_yellow_cards": 2,
        "away_yellow_cards": 2,
        "home_red_cards": 0,
        "away_red_cards": 0,
        "home_goalkeeper_saves": 3,
        "away_goalkeeper_saves": 2,
        "home_xgot_faced": 1.95,
        "away_xgot_faced": 2.44,
        "home_goals_prevented": -0.05,
        "away_goals_prevented": -1.56,
    }

    for field, expected in expected_values.items():
        actual = getattr(snapshot, field)
        assert actual == expected, (
            f"Field '{field}' mismatch: expected {expected}, got {actual}"
        )

    assert snapshot.event_id.startswith("snap_"), (
        f"Expected event_id to start with 'snap_', got {snapshot.event_id}"
    )


def test_parse_flashscore_empty_and_broken_payloads(provider: FlashscoreProvider):
    """
    Ensures the parser handles completely empty or broken strings gracefully 
    without throwing IndexError or KeyError.
    """
    # Completely empty text
    assert provider._parse_flashscore_format("", "", "broken_id") == []
    
    # Missing stats dict entirely (no SE tags)
    mock_core = "DA÷3¬DE÷1¬DF÷1¬"
    mock_stats_broken = "RANDOM÷JUNK¬~SOMETHING÷ELSE¬~"
    assert provider._parse_flashscore_format(mock_stats_broken, mock_core, "broken_id") == []


def test_handle_error_strike_triggers_shutdown(provider: FlashscoreProvider):
    """
    Ensures that after max allowed errors (10), the provider forces match status to 3 
    to trigger a graceful container shutdown.
    """
    match_id = "strike_test_id"
    
    # Simulate 9 consecutive errors
    for _ in range(9):
        provider._handle_error_strike(match_id, "Proxy disconnected")
        assert provider.current_match_status != 3
        
    # Strike 10 (Threshold reached)
    provider._handle_error_strike(match_id, "Fatal WAF block")
    
    assert provider.consecutive_errors == 10
    assert provider.current_match_status == 3, "Status should be set to 3 to kill the process."


@patch('services.ingestion_service.src.infrastructure.flashscore_provider.datetime')
def test_parse_flashscore_live_match_time(mock_datetime, provider: FlashscoreProvider):
    """
    Ensures the 2nd half time is calculated correctly based on absolute timestamps.
    """
    # Set a fixed "current time" to freeze the environment
    mock_now = MagicMock()
    mock_now.timestamp.return_value = 1700002000 # Pretend it is exactly this second
    mock_datetime.now.return_value = mock_now
    mock_datetime.UTC = UTC 

    # DD = 1700001400 (Start of 2nd half). 
    # Difference = 1700002000 - 1700001400 = 600 seconds (exactly 10 minutes).
    # Expected minute: 45 + 10 = 55th minute.
    mock_core_live = "DA÷1¬DB÷13¬DD÷1700001400¬DE÷1¬DF÷0¬"
    mock_stats_minimal = "SE÷Match¬~SG÷Ball possession¬SH÷50%¬SI÷50%¬~"

    result = provider._parse_flashscore_format(mock_stats_minimal, mock_core_live, "live_id")
    
    assert len(result) == 1
    snapshot = result[0]
    
    assert snapshot.minute == 55, "Time math failed to calculate the 55th minute."
    assert snapshot.second == 0


@pytest.mark.asyncio
@patch('services.ingestion_service.src.infrastructure.flashscore_provider.FlashscoreProvider._fetch_text')
async def test_fetch_latest_events_timeout(mock_fetch_text, provider: FlashscoreProvider):
    """
    Ensures that a network timeout results in an empty list, protecting the ingestion loop
    from crashing when Flashscore servers are slow.
    """
    provider.session = MagicMock() # Trick the provider into thinking it's connected
    
    # Force the internal network call to raise an asyncio TimeoutError
    mock_fetch_text.side_effect = asyncio.TimeoutError("Connection timed out")
    
    result = await provider.fetch_latest_events("timeout_id")
    
    assert result == [], "Parser should swallow the timeout and return an empty list."