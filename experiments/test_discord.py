import os
import requests
from dotenv import load_dotenv

load_dotenv()

WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL_TEST")

if not WEBHOOK_URL:
    print("DISCORD_WEBHOOK_URL_TEST couldn't be loaded from .env!")
    exit(1)

payload = {
    "content": "🚀 Test alert."
}

try:    
    # Send POST request to Discord webhook
    response = requests.post(WEBHOOK_URL, json=payload)

    # Discord zwraca kod 204 (No Content), gdy wiadomość przejdzie pomyślnie
    if response.status_code == 204:
        print("Sukces! Sprawdź swój kanał na Discordzie.")
    else:
        print(f"Błąd wysyłania. Kod: {response.status_code}, Odpowiedź: {response.text}")
except Exception as e:
    print("Error sending message to Discord:", e)