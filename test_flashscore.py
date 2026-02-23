import asyncio
import logging
from services.ingestion_service.src.infrastructure.flashscore_provider import FlashscoreProvider

# Ustawiamy logowanie, żeby widzieć ewentualne błędy i nasz debug surowego tekstu
logging.basicConfig(level=logging.DEBUG)

async def run_test() -> None:
    print("Inicjalizacja bezpiecznego sniffera...")
    provider = FlashscoreProvider()
    
    await provider.connect()

    # ID z Twojego poprzedniego cURL-a
    match_id = "fwrFZdIs" 
    print(f"\nPobieranie surowych statystyk dla meczu: {match_id}...\n")

    try:
        events = await provider.fetch_latest_events(match_id)
        
        if events:
            print("\n✅ SUKCES! Zwalidowany Snapshot Pydantic:\n")
            for event in events:
                # model_dump_json(indent=4) wydrukuje nam pięknego, sformatowanego JSON-a
                print(event.model_dump_json(indent=4))
        else:
            print("\n⚠️ Otrzymano pustą listę. Sprawdź logi (może token x-fsign wygasł?).")
            
    finally:
        await provider.disconnect()
        print("\nSesja HTTP zamknięta.")

if __name__ == "__main__":
    asyncio.run(run_test())