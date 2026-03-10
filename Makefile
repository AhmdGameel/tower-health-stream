COMPOSE = docker compose -f docker/docker-compose.yml

.PHONY: up down logs ps dbt-run clean

up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=50

dbt-run:
	docker exec ths-analytics-runner python services/analytics_runner.py
