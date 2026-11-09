COMPOSE ?= docker compose
SPARK_MASTER ?= spark://spark-master:7077
SPARK_SUBMIT = $(COMPOSE) exec spark-master /opt/spark/bin/spark-submit --master $(SPARK_MASTER)

.PHONY: help up down restart ps logs master-ui history-ui jupyter-url \
        shell pyspark sql submit pi wordcount sales test clean-output

help: ## Show available targets
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Start the whole stack in the background
	$(COMPOSE) up -d

down: ## Stop and remove the containers (data is kept)
	$(COMPOSE) down

restart: ## Restart all containers
	$(COMPOSE) restart

ps: ## Show container status and ports
	$(COMPOSE) ps

logs: ## Follow logs from all services
	$(COMPOSE) logs -f --tail=100

master-ui: ## Open the Spark master UI
	xdg-open http://localhost:8080

history-ui: ## Open the Spark History Server UI
	xdg-open http://localhost:18080

jupyter-url: ## Print the JupyterLab URL
	@token=$$(grep -E '^JUPYTER_TOKEN=' .env | cut -d= -f2); \
		echo "http://localhost:8888/?token=$${token:-spark}"

shell: ## Open a bash shell in the master container
	$(COMPOSE) exec spark-master bash

pyspark: ## Open an interactive PySpark shell connected to the cluster
	$(COMPOSE) exec spark-master /opt/spark/bin/pyspark --master $(SPARK_MASTER)

sql: ## Open an interactive Spark SQL shell
	$(COMPOSE) exec spark-master /opt/spark/bin/spark-sql --master $(SPARK_MASTER)

submit: ## Submit the sales report job (override APP=... to submit another)
	$(SPARK_SUBMIT) /opt/spark-apps/$(or $(APP),sales_report.py)

pi: ## Run the Pi estimation job
	$(SPARK_SUBMIT) /opt/spark-apps/pi.py 16

wordcount: ## Run the word count job
	$(SPARK_SUBMIT) /opt/spark-apps/wordcount.py

sales: ## Run the sales report job
	$(SPARK_SUBMIT) /opt/spark-apps/sales_report.py

test: pi wordcount sales ## Run all three sample jobs
	@echo "All sample jobs finished. Check http://localhost:18080"

clean-output: ## Delete generated outputs and event logs (keeps source data)
	docker compose run --rm --no-deps --user root --entrypoint /bin/bash spark-master -c \
		'rm -rf /opt/spark-data/output /opt/spark-data/warehouse; \
		 find /opt/spark-events -mindepth 1 ! -name .gitkeep -delete'
	@echo "Removed data/output, data/warehouse and event logs."
