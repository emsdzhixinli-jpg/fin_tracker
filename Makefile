# FinTransTracker local development helpers.
#
# Usage:
#   make kafka                 # start broker + topics
#   make producer              # terminal 2 — interactive producer CLI
#   make consumer              # terminal 3 — interactive consumer CLI
#   make kafka-down            # stop Kafka services
#   make -f Makefile.test unit # see Makefile.test for test targets

PYTHON ?= ./venv/bin/python
export PYTHONPATH := src

ifneq (,$(wildcard .env))
include .env
export
endif

KAFKA_BOOTSTRAP_SERVERS ?= localhost:9092
export KAFKA_BOOTSTRAP_SERVERS

.PHONY: kafka kafka-down producer consumer help

help:
	@echo "make kafka      - start Kafka + create topics (detached)"
	@echo "make producer   - run producer CLI on the host"
	@echo "make consumer   - run consumer CLI on the host"
	@echo "make kafka-down - stop Kafka services"
	@echo "make -f Makefile.test unit|integration|all - run tests"

kafka:
	docker compose up -d kafka kafka-init

kafka-down:
	docker compose stop kafka kafka-init

producer:
	$(PYTHON) -m services.producer

consumer:
	$(PYTHON) -m services.consumer
