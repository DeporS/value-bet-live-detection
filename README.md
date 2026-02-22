# value-bet-live-detection

## Installing poetry
- curl -sSL https://install.python-poetry.org | python3 -
- export PATH="$HOME/.local/bin:$PATH"
- source ~/.bashrc

Go to project folder
- poetry install

## Run main.py
PYTHONPATH=$(pwd) poetry run python services/ingestion_service/src/main.py