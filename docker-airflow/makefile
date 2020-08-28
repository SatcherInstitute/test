.PHONY: run build

pull:
	docker pull puckel/docker-airflow

build: pull
	docker build --rm -t puckel/docker-airflow .

run: build
	docker-compose -f docker-compose-LocalExecutor.yml up -d
	@echo airflow running on http://localhost:8080