import re
import logging
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fetch_dynamic_matches():
    target_url = "https://www.flashscore.com/"
    
    with sync_playwright() as p:
        logger.info("Launching Chromium...")
        # headless=True so we don't open a visible browser window during testing
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        logger.info(f"Entering: {target_url} and waiting for network idle...")
        # Enter the URL and wait until the network is idle
        page.goto(target_url, wait_until="networkidle")
        
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
    match_elements = soup.find_all('div', class_=re.compile(r'event__match--(scheduled|live)'))
    
    logger.info(f"Found {len(match_elements)} rendered divs with matches.")

    daily_matches = []
    for element in match_elements:
        raw_id = element.get('id', '')

        if raw_id.startswith('g_1_'):
            match_id = raw_id[4:]  
            
            # Regex validation to ensure we only keep safe and expected match IDs
            if re.match(r'^[a-zA-Z0-9]{8}$', match_id):
                time_div = element.find('div', class_='event__time')
                match_time = time_div.text.strip() if time_div else "Unknown"

                home_div = element.find('div', class_='event__homeParticipant')
                home_team = home_div.get_text(strip=True) if home_div else "Unknown"

                away_div = element.find('div', class_='event__awayParticipant')
                away_team = away_div.get_text(strip=True) if away_div else "Unknown"

                daily_matches.append({
                    'match_id': match_id,
                    'scheduled_time': match_time,
                    'home_team': home_team,
                    'away_team': away_team
                })
            else:
                logger.warning(f"Rejected invalid ID: {raw_id}")
    
    logger.info(f"Finally found {len(daily_matches)} valid and safe match IDs.")

    for match in daily_matches[:5]:
        logger.info(f"ID={match['match_id']}, Scheduled Time={match['scheduled_time']}, {match['home_team']} vs {match['away_team']}")
        
    return daily_matches

if __name__ == "__main__":
    test_fetch_dynamic_matches()