.PHONY: help up build dwh clickhouse all extras-up cassandra mongo neo4j valkey extras ps logs check down clean

DC ?= docker compose
SPARK_SERVICE ?= spark
SPARK_SUBMIT ?= spark-submit

PG_PORT_EXTERNAL ?= 5432
export PG_PORT_EXTERNAL

SERVICE ?=

help: ## Show available commands
	@awk 'BEGIN {FS=":.*##"} /^[a-zA-Z0-9_-]+:.*##/ {printf "%-16s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

up: ## Start Postgres + ClickHouse (wait for healthy)
	$(DC) up -d --wait postgres clickhouse

build: ## Build Spark image
	$(DC) build $(SPARK_SERVICE)

dwh: up build ## Load star schema into Postgres (dwh.*)
	$(DC) run --rm $(SPARK_SERVICE) $(SPARK_SUBMIT) /opt/jobs/01_raw_to_dwh.py

clickhouse: dwh ## Build 6 marts in ClickHouse (reports.*)
	$(DC) run --rm $(SPARK_SERVICE) $(SPARK_SUBMIT) /opt/jobs/02_dwh_to_clickhouse.py

all: clickhouse ## Run mandatory pipeline: raw -> dwh -> clickhouse

extras-up: up ## Start optional DBs (Cassandra/Mongo/Neo4j/Valkey)
	$(DC) --profile extras up -d --wait cassandra mongo neo4j valkey

cassandra: extras-up dwh ## Build marts in Cassandra (optional)
	$(DC) run --rm $(SPARK_SERVICE) $(SPARK_SUBMIT) /opt/jobs/03_dwh_to_cassandra.py

mongo: extras-up dwh ## Build marts in MongoDB (optional)
	$(DC) run --rm $(SPARK_SERVICE) $(SPARK_SUBMIT) /opt/jobs/04_dwh_to_mongo.py

neo4j: extras-up dwh ## Build marts in Neo4j (optional)
	$(DC) run --rm $(SPARK_SERVICE) $(SPARK_SUBMIT) /opt/jobs/05_dwh_to_neo4j.py

valkey: extras-up dwh ## Build marts in Valkey (optional)
	$(DC) run --rm $(SPARK_SERVICE) $(SPARK_SUBMIT) /opt/jobs/06_dwh_to_valkey.py

extras: cassandra mongo neo4j valkey ## Run all optional marts

ps: ## Show docker compose status
	$(DC) --profile extras ps

logs: ## Tail logs (SERVICE=postgres|clickhouse|spark|...)
	$(DC) --profile extras logs -f $(SERVICE)

check: up ## Quick counts in Postgres + ClickHouse
	$(DC) exec -T postgres psql -U bigdata -d bigdata -c "SELECT 'public.mock_data' AS table, COUNT(*) FROM public.mock_data UNION ALL SELECT 'dwh.fact_sales', COUNT(*) FROM dwh.fact_sales UNION ALL SELECT 'dwh.dim_customer', COUNT(*) FROM dwh.dim_customer UNION ALL SELECT 'dwh.dim_seller', COUNT(*) FROM dwh.dim_seller UNION ALL SELECT 'dwh.dim_product', COUNT(*) FROM dwh.dim_product UNION ALL SELECT 'dwh.dim_store', COUNT(*) FROM dwh.dim_store UNION ALL SELECT 'dwh.dim_supplier', COUNT(*) FROM dwh.dim_supplier UNION ALL SELECT 'dwh.dim_date', COUNT(*) FROM dwh.dim_date;"
	$(DC) exec -T clickhouse clickhouse-client --query "SHOW TABLES FROM reports"
	$(DC) exec -T clickhouse clickhouse-client --query "SELECT 'mart_sales_products' AS table, count() FROM reports.mart_sales_products UNION ALL SELECT 'mart_sales_customers', count() FROM reports.mart_sales_customers UNION ALL SELECT 'mart_sales_time', count() FROM reports.mart_sales_time UNION ALL SELECT 'mart_sales_stores', count() FROM reports.mart_sales_stores UNION ALL SELECT 'mart_sales_suppliers', count() FROM reports.mart_sales_suppliers UNION ALL SELECT 'mart_product_quality', count() FROM reports.mart_product_quality;"

down: ## Stop all services (including extras)
	$(DC) --profile extras down --remove-orphans

clean: ## Stop services and remove volumes (data reset)
	$(DC) --profile extras down -v --remove-orphans
