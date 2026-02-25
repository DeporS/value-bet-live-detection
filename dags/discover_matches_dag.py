import re
import logging
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

def fetch_daily_matches():
    """
    Fetches the list of matches once a day
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    target_url = "https://www.flashscore.com/"

    try:
        logger.info(f"Fetching matches from {target_url}")
        response = requests.get(target_url, headers=headers, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors

        soup = BeautifulSoup(response.text, 'html.parser')

        # Look for scheduled matches
        match_elements = soup.find_all('div', class_=re.compile(r'event__match--scheduled'))

        daily_matches = []
        for element in match_elements:
            raw_id = element.get('id', '')

            if raw_id.startswith('g_1_'):
                match_id = raw_id[4:]  # Remove 'g_1_' prefix

                # Regex protection
                if re.match(r'^[a-zA-Z0-9]{8}$', clean_id):
                    time_div = element.find('div', class_='event__time')
                    match_time = time_div.text.strip() if time_div else "Unknown"

                    daily_matches.append({
                        'match_id': match_id,
                        'scheduled_time': match_time
                    })
                else:
                    logger.warning(f"Skipping invalid match ID: {raw_id}")
        
        logger.info(f"Found {len(daily_matches)} scheduled matches.")

        # Log 5 sample matches for verification
        for match in daily_matches[:5]:
            logger.info(f"Sample match: ID={match['match_id']}, Scheduled Time={match['scheduled_time']}")
        
        return daily_matches
    
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching matches: {e}")
        raise

# Define the DAG
default_args = {
    'owner': 'DeporS',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'discover_daily_matches',
    default_args=default_args,
    description='Scrapes Flashscore at 0:01 to discover scheduled matches and their IDs',
    schedule_interval='1 0 * * *',  # At 00:01 every day
    start_date=datetime(2026, 2, 25),
    catchup=False,
    tags=['scraping', 'discovery']
) as dag:

    scrape_task = PythonOperator(
        task_id='scrape_matches',
        python_callable=fetch_daily_matches
    )