## Makefile for src/movies_pipeline_practice file

SHELL := /bin/bash


initvenv: 
	python3.12 -m venv venv
	source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

.PHONY: test
test:
	source venv/bin/activate && pytest tests/ -v --disable-warnings

run:
	spark-submit src/movies_pipeline_practice.py

.PHONY: run-debug
run-debug:
	spark-submit src/movies_pipeline_practice.py --debug --log-level DEBUG 

.PHONY: run-with-config
run-with-config:
	spark-submit src/movies_pipeline_practice.py --config config/config.yaml

.PHONY: run-with-config-debug
run-with-config-debug:
	spark-submit src/movies_pipeline_practice.py --log-level DEBUG --config conf.yaml 


# startservices: 					## Starts spark docker services
# 	docker-compose up -d

# lsservices:						## Lists spark docker services
# 	docker ps --filter 'name=spark'

# #restartservices:				## Restarts spark docker services
# 	docker-compose down && docker-compose up -d