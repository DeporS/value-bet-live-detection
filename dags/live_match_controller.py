import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.sensors.date_time import DateTimeSensor
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.models import Variable

# Default arguments for the DAG
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
    description='Dynamically spawns Docker containers for live football matches precisely before kick-off',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 26),
    catchup=False,
    tags=['live_betting', 'ingestion', 'docker'],
) as dag:

    @task
    def extract_matches_from_xcom(**context) -> list[dict]:
        """
        Pulls the json array of todays matches from previous Airflow task via XCom.
        Expected JSON format: [{"match_id": "...", "start_time": "YYYY-MM-DDTHH:MM:SS"}, ...]
        """
        matches_json = context['ti'].xcom_pull(task_ids='discover_daily_matches')

        if not matches_json:
            raise ValueError("No match data found in XCom. Ensure the discover_daily_matches task is running and pushing data correctly.")
        
        try:
            parsed_matches = json.loads(matches_json)
        except json.JSONDecodeError:
            raise ValueError(f"Failed to parse JSON from XCom: {matches_json}")

        return parsed_matches

    @task
    def calculate_wakeup_time(match_data: dict) -> str:
        """
        Calculates the exact datetime to wake up the sensor (2 minutes before kick-off).
        """
        start_time = datetime.fromisoformat(match_data["start_time"])
        wakeup_time = start_time - timedelta(minutes=2)
        return wakeup_time.isoformat()

    @task
    def build_container_environment(match_data: dict) -> dict:
        """
        Constructs the secure environment variable dictionary for each specific container.
        Fetches the sensitive Proxy URL securely from Airflow Variables.
        """
        proxy_url = Variable.get("PROXY_URL", default_var="http://fallback-proxy:8080")

        return {
            "MATCH_ID": match_data["match_id"],
            "PROXY_URL": proxy_url,
            "KAFKA_BROKER": "kafka:9092" # Internal Docker network hostname
        }
    
    # 1. Fetch the list of matches
    matches_list = extract_matches_from_xcom()

    # 2. Prepare mapped payloads for time and environment
    wakeup_times = calculate_wakeup_time.expand(match_data=matches_list)
    env_payloads = build_container_environment.expand(match_data=matches_list)

    # 3. Dynamic Task Mapping: Sensor
    # mode='reschedule' prevents Airflow workers from being locked up while waiting hours for a match
    wait_for_kickoff = DateTimeSensor.partial(
        task_id='wait_for_kickoff',
        mode='reschedule',
        poke_interval=30, # Wake up and check the time every 30 seconds
    ).expand(target_time=wakeup_times)

    # 4. Dynamic Task Mapping: Docker
    spawn_ingestion_containers = DockerOperator.partial(
        task_id='run_match_ingestion',
        image='value-bet-live-detection-ingestion-service:latest',
        api_version='auto',
        auto_remove='force', 
        network_mode='value-bet-live-detection_value_engine_net',
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False,
    ).expand(environment=env_payloads)

    # 5. Define the execution flow 
    wait_for_kickoff >> spawn_ingestion_containers