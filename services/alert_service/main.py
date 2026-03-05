import os
import json
import logging
import requests
from confluent_kafka import Consumer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("AlertService")

def send_alert(webhook_url: str, message_content: str) -> None:
    try:
        response = requests.post(
            webhook_url, 
            json={"content": message_content}, 
            timeout=3.0
        )
        if response.status_code == 204:
            logger.info("Alert sent successfully.")
        else:
            logger.warning(f"Failed to send alert. Status code: {response.status_code}, Response: {response.text}")
    except requests.RequestException as e:
        logger.error(f"Error sending alert: {e}")


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
                current_minute = event.get("minute", 0)
                current_second = event.get("second", 0)
                half = 1 if event.get("granular_status", 0) == 12 else 2 if event.get("granular_status", 0) == 13 else 0

                # Initialize state for new matches
                if match_id not in match_states:
                    match_states[match_id] = {"home": current_home, "away": current_away, "status": current_status, "already_started": False, "zero_strikes": 0}
                    continue

                if current_second > 3:
                    current_minute += 1
                
                # Handle minute display for alerts, especially around halftime and fulltime
                minute_info = str(current_minute)
                if half == 1 and current_minute >= 45:
                    minute_info = "45+" + str(current_minute - 45)
                elif half == 2 and current_minute >= 90:
                    minute_info = "90+" + str(current_minute - 90)

                last_state = match_states[match_id]
                last_home = last_state["home"]
                last_away = last_state["away"]
                last_status = last_state["status"]

                # --- Match started ---
                if current_status == 12 and match_states[match_id]["already_started"] == False: # Status 12 indicates match has started
                    title = "🚀 - **MECZ ROZPOCZĘTY!**"
                    msg_content = (
                        f"----------------------------------\n"
                        f"{title}\n"
                        f"{home_team} vs {away_team}\n"
                        f"⏱️ {minute_info}'  |  **{current_home} - {current_away}**\n"
                        f"----------------------------------\n"
                    )
                    # dispatch alert to Discord
                    send_alert(webhook_url, msg_content)
                    logger.info(f"Send alert for started match: {current_home} - {current_away}")
                    match_states[match_id]["already_started"] = True

                # --- Match finished ---
                elif current_status == 3 and last_status != 3:
                    title = "🏁 - **KONIEC MECZU**"
                    msg_content = (
                        f"----------------------------------\n"
                        f"{title}\n"
                        f"{home_team} vs {away_team}\n"
                        f"**{current_home} - {current_away}**\n"
                        f"----------------------------------\n"
                    )
                    # dispatch alert to Discord
                    send_alert(webhook_url, msg_content)
                    logger.info(f"Send alert for finished match: {current_home} - {current_away}")

                # --- Main logic ---
                if current_home != last_home or current_away != last_away:
                    
                    # Jitter check
                    if current_home == 0 and current_away == 0 and (last_home > 0 or last_away > 0):
                        # Suspicious zeros
                        match_states[match_id]["zero_strikes"] = match_states[match_id].get("zero_strikes", 0) + 1
                        
                        if match_states[match_id]["zero_strikes"] < 3: 
                            logger.warning(f"Suspicious zero update for match {match_id}.")
                            continue
                        else:
                            logger.warning(f"Multiple zero updates for match {match_id}. Accepting update.")

                    
                    # Check if its a new goal or a correction (VAR)
                    if current_home > last_home or current_away > last_away:
                        title = "⚽ - **GOL!**"
                        msg_content = (
                            f"----------------------------------\n"
                            f"{title}\n"
                            f"{home_team} vs {away_team}\n"
                            f"⏱️ {minute_info}'  |  **{current_home} - {current_away}**\n"
                            f"----------------------------------\n"
                        )
                    else:
                        title = "🚨 - **KOREKTA!**"
                        msg_content = (
                            f"----------------------------------\n"
                            f"{title}\n"
                            f"{home_team} vs {away_team}\n"
                            f"⏱️ {minute_info}'\n"
                            f"**{last_home} - {last_away}** → **{current_home} - {current_away}**\n"
                            f"----------------------------------\n"
                        )
                    
                    # dispatch alert to Discord
                    send_alert(webhook_url, msg_content)
                    logger.info(f"Sent alert to discord: {current_home} - {current_away}")

                match_states[match_id] = {
                    "home": current_home, 
                    "away": current_away, 
                    "status": current_status,
                    "already_started": match_states[match_id].get("already_started", False),
                    "zero_strikes": 0 # reset zero strikes on any valid update
                }

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