include .env

install:
	python3 -V \
	&& python3 -m venv .venv \
	&& source .venv/bin/activate \
	&& pip install --upgrade pip && pip install -r requirements.txt

build-airflow:
	docker compose -f airflow-docker-compose.yml build --quiet

up-airflow:
	docker compose -f airflow-docker-compose.yml --env-file .env up -d

down-airflow:
	docker compose -f airflow-docker-compose.yml --env-file .env down

restart-airflow:
	docker compose -f airflow-docker-compose.yml --env-file .env down && docker compose -f airflow-docker-compose.yml --env-file .env up -d