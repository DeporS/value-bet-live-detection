from datetime import datetime
import re
import logging
import time
import random
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_fetch_dynamic_matches():
    target_url = "https://www.flashscore.com/"
    daily_matches = []
    
    # Wszystko, co wymaga przeglądarki, musi być wewnątrz tego bloku 'with'
    with sync_playwright() as p:
        logger.info("Launching Chromium...")
        # headless=True, aby nie otwierać widocznego okna
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        
        logger.info(f"Entering: {target_url} and waiting for DOM content...")
        page.goto(target_url, wait_until="domcontentloaded", timeout=60000)
        
        try:
            # Czekamy na mecze i scrollujemy
            page.wait_for_selector('.event__match', timeout=15000)
            logger.info("First matches appeared. Allowing SPA to fully render...")
            page.wait_for_timeout(5000)
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1000)
            logger.info("Successfully waited for all match elements to populate.")
            
        except Exception as e:
            logger.error("Timeout: Match elements did not appear within 15 seconds.")
            page.close()
            browser.close()
            return []
            
        # Zapisujemy HTML głównej strony i zamykamy pierwszą kartę (oszczędność RAM)
        html_content = page.content()
        page.close()

        # Parsowanie BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        match_elements = soup.find_all('div', class_=re.compile(r'event__match--(scheduled|live)'))
        
        logger.info(f"Found {len(match_elements)} rendered divs with matches.")

        for element in match_elements:
            raw_id = element.get('id', '')

            if raw_id.startswith('g_1_'):
                match_id = raw_id[4:]  
                
                # Walidacja Regex ID
                if re.match(r'^[a-zA-Z0-9]{8}$', match_id):
                    time_div = element.find('div', class_='event__time')
                    match_time = time_div.text.strip() if time_div else "Unknown"

                    home_div = element.find('div', class_='event__homeParticipant')
                    home_team = home_div.get_text(strip=True) if home_div else "Unknown"

                    away_div = element.find('div', class_='event__awayParticipant')
                    away_team = away_div.get_text(strip=True) if away_div else "Unknown"

                    # Inicjujemy słownik, od razu dodając puste klucze dla oddsów
                    daily_matches.append({
                        'match_id': match_id,
                        'scheduled_time': match_time,
                        'home_team': home_team,
                        'away_team': away_team,
                        'odds_home': None,
                        'odds_draw': None,
                        'odds_away': None
                    })
                else:
                    logger.warning(f"Rejected invalid ID: {raw_id}")
        
        logger.info(f"Preparing to fetch odds for {len(daily_matches)} valid matches...")

        # TWORZYMY ODDS_PAGE - Otwieramy nową kartę specjalnie na kursy, 
        # wciąż będąc wewnątrz otwartej przeglądarki (browser)
        odds_page = browser.new_page()

        for match in daily_matches:
            # Omijamy mecze, które już trwają (wymaga sprawdzenia czy to format z 'T')
            logger.info(f"Processing match {match['match_id']} scheduled at {match['scheduled_time']}...")
            if match['scheduled_time'] > datetime.now().strftime("%Y-%m-%dT%H:%M:%S"):
                short_url = f"https://www.flashscore.com/match/{match['match_id']}"
                
                try:
                    logger.info(f"Resolving full URL for {match['match_id']}...")
                    odds_page.goto(short_url, wait_until="domcontentloaded", timeout=30000)
                    
                    resolved_url = odds_page.url
                    
                    if "?mid=" in resolved_url:
                        base_part, query_part = resolved_url.split("?", 1)
                        odds_url = f"{base_part}odds/1x2-odds/full-time/?{query_part}"
                        
                        logger.info(f"Navigating to odds URL: {odds_url}")
                        odds_page.goto(odds_url, wait_until="domcontentloaded", timeout=30000)
                        
                        # Czekamy na tabelę z kursami
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
                    else:
                        logger.warning(f"Unexpected URL structure for {match['match_id']}: {resolved_url}")
                
                except Exception as e:
                    logger.warning(f"Could not extract odds for {match['match_id']}. Skipping. Reason: {str(e)}")
                
                # Zabezpieczenie przed oflagowaniem (Rate Limiting)
                time.sleep(random.uniform(1.5, 3.0))

        # Bezpiecznie zamykamy kartę od kursów i całą przeglądarkę na samym końcu procesu
        odds_page.close()
        browser.close()

    logger.info("Browser closed. Printing sample results:")
    for match in daily_matches[:5]:
        logger.info(f"ID={match['match_id']}, {match['home_team']} vs {match['away_team']} | Odds: {match['odds_home']} / {match['odds_draw']} / {match['odds_away']}")
        
    return daily_matches

if __name__ == "__main__":
    test_fetch_dynamic_matches()