import asyncio
import logging
import time
from services.ingestion_service.src.infrastructure.flashscore_provider import FlashscoreProvider

logging.basicConfig(level=logging.INFO)

async def run_test() -> None:
    print("Initializing FlashscoreProvider and connecting...")
    provider = FlashscoreProvider()
    
    await provider.connect()

    # match id from flashscore
    match_id = "KbUrxW1T" 
    print(f"\nFetching raw stats for match ID: {match_id}...\n")

    iteration = 1
    try:
        while True:
            current_time = time.strftime('%X')
            print(f"\n--- Iteration {iteration} at {current_time} ---")

            events = await provider.fetch_latest_events(match_id)
            
            if events:
                print("\nSuccess, events retrieved:\n")
                for event in events:
                    # model_dump_json(indent=4) prints readable JSON
                    print(event.model_dump_json(indent=4))

                    # Print just the most important stats in a readable format
                    print(f"{event.minute}:{event.second} | "
                          f"Goals: {event.home_goals}:{event.away_goals} | "
                          f"Possession: {event.home_possession * 100:.0f}% - {event.away_possession * 100:.0f}% | "
                          f"xG: {event.home_xg} - {event.away_xg}")
            else:
                print("\nGot empty list. Check logs (x-fsign token may have expired?).")
            
            iteration += 1
            await asyncio.sleep(5)  # Wait before next fetch to avoid spamming
            
    finally:
        await provider.disconnect()
        print("\nHTTP session closed.")

if __name__ == "__main__":
    try:
        asyncio.run(run_test())
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")