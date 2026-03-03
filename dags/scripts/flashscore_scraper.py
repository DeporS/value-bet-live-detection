import re
import logging
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def fetch_daily_matches(**kwargs) -> list:
    """
    Fetches the list of top matches once a day, strictly from the primary soccer container.
    """
    target_url = "https://www.flashscore.com/"

    try:
        with sync_playwright() as p:
            logger.info("Launching Chromium...")
            # headless=True so we don't open a visible browser window during testing
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            
            logger.info(f"Entering: {target_url} and waiting for network idle...")
            # Enter the URL and wait until the DOM content is loaded
            page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
            
            try:
                # Wait up to 15 seconds for the match elements to appear in the DOM
                page.wait_for_selector('.event__match', timeout=15000)
                logger.info("Successfully waited for match elements to appear.")
            except Exception as e:
                logger.error("Timeout: Match elements did not appear within 15 seconds.")
                browser.close()
                return
                
            # Extract the full HTML content after JavaScript has rendered the page
            html_content = page.content()
            browser.close()

        # Use BeautifulSoup to parse the rendered HTML and find match elements
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Find ONLY the first main 'sportName soccer' container
        # This automatically ignores 'sportNews' and lower-tier soccer divs
        main_soccer_container = soup.find('div', class_='sportName soccer')
        
        if not main_soccer_container:
            logger.warning("Could not find the main 'sportName soccer' container. DOM might have changed.")
            return []

        # Find match elements STRICTLY inside the isolated main container
        match_elements = main_soccer_container.find_all('div', class_=re.compile(r'event__match--(scheduled|live)'))
        
        logger.info(f"Found {len(match_elements)} rendered divs with matches.")

        daily_matches = []
        today_date = datetime.now().strftime("%Y-%m-%d") # Needed for ISO format building

        for element in match_elements:
            raw_id = element.get('id', '')

            if raw_id.startswith('g_1_'):
                match_id = raw_id[4:]  
                
                # Regex validation to ensure we only keep safe and expected match IDs
                if re.match(r'^[a-zA-Z0-9]{8}$', match_id):
                    time_div = element.find('div', class_='event__time')
                    raw_time = time_div.text.strip() if time_div else "Unknown"

                    # Look for standard "HH:MM" format
                    time_match = re.search(r'(\d{2}):(\d{2})', raw_time)

                    # Convert raw time (e.g., "21:00") into ISO 8601 format required by Airflow Sensor
                    # Note: We assume the match is today since it's a daily scraper
                    if time_match:
                        # Match is scheduled for later today
                        hours, minutes = time_match.groups()
                        formatted_start_time = f"{today_date}T{hours}:{minutes}:00"
                    elif raw_time:
                        # Time text exists but it's not HH:MM (e.g., "15'", "HT").
                        # This means the match is likely already live. 
                        # Set start time to NOW so Airflow sensor passes immediately.
                        logger.info(f"Match {match_id} appears live ('{raw_time}'). Scheduling immediately.")
                        formatted_start_time = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
                    else:
                        # Complete parsing failure. 
                        # Skip this match completely to prevent Airflow zombie containers.
                        logger.warning(f"No time data for {match_id}. Skipping to protect resources.")
                        continue # Skips the rest of the loop and moves to the next match

                    home_div = element.find('div', class_='event__homeParticipant')
                    home_team = home_div.get_text(strip=True) if home_div else "Unknown"

                    away_div = element.find('div', class_='event__awayParticipant')
                    away_team = away_div.get_text(strip=True) if away_div else "Unknown"

                    daily_matches.append({
                        'match_id': match_id,
                        'scheduled_time': formatted_start_time,
                        'home_team': home_team,
                        'away_team': away_team
                    })
                else:
                    logger.warning(f"Rejected invalid ID: {raw_id}")
        
        logger.info(f"Finally found {len(daily_matches)} valid and safe match IDs.")

        for match in daily_matches[:5]:
            logger.info(f"ID={match['match_id']}, Scheduled Time={match['scheduled_time']}, {match['home_team']} vs {match['away_team']}")
            
        return daily_matches
    
    except Exception as e:
        logger.error(f"Error fetching matches: {e}")
        raise