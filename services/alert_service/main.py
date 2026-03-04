import os
import json
import logging
import requests
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AlertService")

def main() -> None:
    webhook_url = os.getenv("DISCORD_GOAL_ALERT_WEBHOOK_URL")
    kafka_broker = os.getenv("KAFKA_BROKER", "localhost:9092")

    if not webhook_url:
        logger.error("ENV VAR DISCORD_GOAL_ALERT_WEBHOOK_URL is not set. Stopping service.")
        return

    # Kafka consumer configuration
    conf = {
        'bootstrap.servers': kafka_broker,
        'group.id': 'discord_live_alerts_group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }
    consumer = Consumer(conf)
    consumer.subscribe(['raw_match_events'])

    # Local state
    # {"match_id": {"home": 0, "away": 0, "status": 0}}
    match_states = {}

    logger.info("Alert service started. Listening for match events...")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None or msg.error():
                continue

            try:
                # Entry validation and parsing
                event = json.loads(msg.value().decode('utf-8'))
                
                if event.get("event_type") != "stats_snapshot":
                    continue

                match_id = event["match_id"]
                home_team = event.get("home_team", "Dom")
                away_team = event.get("away_team", "Wyjazd")
                
                current_home = event.get("home_goals", 0)
                current_away = event.get("away_goals", 0)
                current_status = event.get("match_status", 0)

                # Initialize state for new matches
                if match_id not in match_states:
                    match_states[match_id] = {"home": current_home, "away": current_away, "status": current_status}
                    continue

                last_state = match_states[match_id]
                last_home = last_state["home"]
                last_away = last_state["away"]
                last_status = last_state["status"]

                # --- Match finished ---
                if current_status == 3 and last_status != 3:
                    msg_content = f"🏁 **KONIEC MECZU** | {home_team} vs {away_team} | Wynik: **{current_home} - {current_away}**"
                    
                    match_states[match_id]["status"] = 3 # Spam prevention flag
                    requests.post(
                        webhook_url, 
                        json={"content": msg_content}, 
                        timeout=3.0
                    )
                    logger.info(f"Wysłano powiadomienie o końcu meczu: {current_home} - {current_away}")
                    continue # Skip goal alert logic for finished matches

                # --- Main logic ---
                if current_home != last_home or current_away != last_away:
                    
                    # Check if its a new goal or a correction (VAR)
                    if current_home > last_home or current_away > last_away:
                        title = "⚽ **GOL!**"
                    else:
                        title = "🚨 **VAR / KOREKTA WYNIKU!**"

                    msg_content = f"{title} | {home_team} vs {away_team} | Wynik: **{current_home} - {current_away}**"
                    
                    # Update goals local state
                    match_states[match_id]["home"] = current_home
                    match_states[match_id]["away"] = current_away

                    # dispatch alert to Discord
                    requests.post(
                        webhook_url, 
                        json={"content": msg_content}, 
                        timeout=3.0 
                    )
                    logger.info(f"Sent alert to discord: {current_home} - {current_away}")

            except json.JSONDecodeError:
                logger.warning("Got invalid JSON.")
            except Exception as e:
                logger.error(f"Unexpected error occurred: {e}")

    except KeyboardInterrupt:
        logger.info("Received stop signal.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()