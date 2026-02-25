# value-bet-live-detection

## Installing poetry
- curl -sSL https://install.python-poetry.org | python3 -
- export PATH="$HOME/.local/bin:$PATH"
- source ~/.bashrc

Go to project folder
- poetry install

## Run main program
PYTHONPATH=$(pwd) poetry run python services/ingestion_service/src/main.py

## Run test_flashscore
PYTHONPATH="$(pwd):$(pwd)/services/ingestion_service/src" poetry run python test_flashscore.py

## Run just the airflow containers
docker compose up postgres-airflow airflow-webserver airflow-scheduler

