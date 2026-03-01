from typing import List, Dict
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.date_time import DateTimeSensor
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

from scripts.flashscore_scraper import fetch_daily_matches

default_args = {
    'owner': 'DeporS',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='live_match_orchestrator',
    default_args=default_args,
    description='Dynamically discovers matches and spawns Docker containers before kick-off',
    schedule_interval='1 0 * * *', # At 00:01 every day
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['live_betting', 'ingestion', 'docker'],
) as dag:

    @task
    def discover_matches() -> List[Dict]:
        """
        Executes the external web scraper. TaskFlow API automatically serializes 
        the returned list and pushes it to XCom securely.
        """
        return fetch_daily_matches()

    @task
    def calculate_wakeup_time(match_data: Dict) -> str:
        """
        Calculates the exact datetime to wake up the sensor (2 minutes before kick-off).
        """
        start_time = datetime.fromisoformat(match_data["scheduled_time"])
        wakeup_time = start_time - timedelta(minutes=2)
        return wakeup_time.isoformat()

    @task
    def build_container_environment(match_data: Dict) -> Dict:
        """
        Constructs the secure environment variable dictionary for the container.
        """
        proxy_url = Variable.get("PROXY_URL", default_var="http://fallback-proxy:8080")
        
        return {
            "MATCH_ID": match_data["match_id"],
            "PROXY_URL": proxy_url,
            "KAFKA_BROKER": "kafka:9092"
        }

    # Fetch the list of matches (Automatically pushed to XCom)
    matches_list = discover_matches()

    # Prepare mapped payloads for time and environment
    wakeup_times = calculate_wakeup_time.expand(match_data=matches_list)
    env_payloads = build_container_environment.expand(match_data=matches_list)

    # Dynamic Task Mapping: Sensor
    wait_for_kickoff = DateTimeSensor.partial(
        task_id='wait_for_kickoff',
        mode='reschedule',
        poke_interval=30,
    ).expand(target_time=wakeup_times)

    # Dynamic Task Mapping: Docker
    spawn_ingestion_containers = DockerOperator.partial(
        task_id='run_match_ingestion',
        image='value-bet-live-detection-ingestion-service:latest',
        api_version='auto',
        auto_remove='force', 
        network_mode='value-bet-live-detection_value_engine_net',
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
    ).expand(environment=env_payloads)

    wait_for_kickoff >> spawn_ingestion_containers