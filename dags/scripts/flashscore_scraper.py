import re
import logging
from datetime import datetime, timedelta
import time
import random
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
                logger.info("First matches appeared. Allowing SPA to fully render...")

                # Wait an additional 5 seconds to ensure all JavaScript-rendered content is loaded
                page.wait_for_timeout(5000)

                # Scroll down
                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")

                page.wait_for_timeout(1000) # Wait for any additional content to load after scrolling
                logger.info("Successfully waited for all match elements to populate.")
                
            except Exception as e:
                logger.error("Timeout: Match elements did not appear within 15 seconds.")
                browser.close()
                return
                
            # Extract the full HTML content after JavaScript has rendered the page
            html_content = page.content()
            page.close()

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
            logger.info(f"Preparing to fetch odds for {len(daily_matches)} valid matches...")

            daily_matches_with_odds = []

            # Open a new page for fetching odds
            odds_page = browser.new_page()

            for match in daily_matches:
                # Skip ongoing matches
                if match['scheduled_time'] > datetime.now().strftime("%Y-%m-%dT%H:%M:%S"):
                    short_url = f"https://www.flashscore.com/match/{match['match_id']}"

                    try:
                        logger.info(f"Resolving full URL for {match['match_id']}...")
                        odds_page.goto(short_url, wait_until="domcontentloaded", timeout=30000)

                        resolved_url = odds_page.url

                        if "?mid=" in resolved_url:
                            base_part, query_part = resolved_url.split("?", 1)
                            odds_url = f"{base_part}odds/1x2-odds/full-time/?{query_part}"
                            
                            odds_page.wait_for_timeout(int(random.uniform(500, 1500))) # random delay before next navigation
                            logger.info(f"Navigating to odds URL: {odds_url}")
                            odds_page.goto(odds_url, wait_until="domcontentloaded", timeout=30000)

                            # Wait for the odds table to load
                            odds_page.wait_for_selector('.ui-table__row', timeout=5000)

                            odds_html = odds_page.content()
                            odds_soup = BeautifulSoup(odds_html, 'html.parser')
                            first_row = odds_soup.find('div', class_='ui-table__row')

                            if first_row:
                                odds_cells = first_row.find_all('a', class_=re.compile(r'oddsCell__odd'))
                                logger.info(f"Found {len(odds_cells)} odds cells for match {match['match_id']}.")
                                
                                if len(odds_cells) >= 3:
                                    match['odds_home'] = float(odds_cells[0].get_text(strip=True) or 0)
                                    match['odds_draw'] = float(odds_cells[1].get_text(strip=True) or 0)
                                    match['odds_away'] = float(odds_cells[2].get_text(strip=True) or 0)
                                    logger.info(f"Odds found: 1({match['odds_home']}) X({match['odds_draw']}) 2({match['odds_away']})")

                                    daily_matches_with_odds.append(match) # Only add matches where we successfully fetched odds
                        else:
                            logger.warning(f"Unexpected URL structure for {match['match_id']}: {resolved_url}")
                    
                    except Exception as e:
                        logger.warning(f"Could not extract odds for {match['match_id']}. Skipping. Reason: {str(e)}")
                    
                    # Safe delay to avoid rate limiting
                    odds_page.wait_for_timeout(int(random.uniform(1500, 3000)))

            # Close the odds page and the browser after processing
            odds_page.close()
            browser.close()

        logger.info("Browser closed. Printing sample results:")
        for match in daily_matches_with_odds[:5]:
            logger.info(f"ID={match['match_id']}, {match['home_team']} vs {match['away_team']} | Odds: {match['odds_home']} / {match['odds_draw']} / {match['odds_away']}")
           
        return daily_matches_with_odds
    
    except Exception as e:
        logger.error(f"Error fetching matches: {e}")
        raise