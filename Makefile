include .env

install:
	python3 -V \
	&& python3 -m venv .venv \
	&& source .venv/bin/activate \
	&& pip install --upgrade pip && pip install -r requirements.txt

build:
	docker compose -f docker-compose.yml build --quiet
up:
	docker compose -f docker-compose.yml --env-file .env up -d
down:
	docker compose -f docker-compose.yml --env-file .env down
restart:
	docker compose -f docker-compose.yml --env-file .env down && docker compose -f docker-compose.yml --env-file .env up -d

# airflow
build-airflow:
	docker compose -f airflow-docker-compose.yml build --quiet

up-airflow:
	docker compose -f airflow-docker-compose.yml --env-file .env up -d

down-airflow:
	docker compose -f airflow-docker-compose.yml --env-file .env down

restart-airflow:
	docker compose -f airflow-docker-compose.yml --env-file .env down && docker compose -f airflow-docker-compose.yml --env-file .env up -d

trigger-dag:
	docker exec -it $(AIRFLOW_CONTAINER_NAME) airflow dags trigger $(DAG_ID)
	
list-dags:
	docker exec -it $(AIRFLOW_CONTAINER_NAME) airflow dags list