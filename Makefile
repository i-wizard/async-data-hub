COMPOSE := docker compose
COMPOSE2 := docker compose -f docker-compose.dbrpl.yaml
SERVICE := api
CMD ?= bash

.PHONY: up down exec shell load-customers simulate-payments

up:
	$(COMPOSE) up --build

up-dbrpl:
	$(COMPOSE2) up

# Wipe the postgres data too with: make down V=1
down:
	$(COMPOSE) down $(if $(V),--volumes)

down-dbrpl:
	$(COMPOSE2) down $(if $(V),--volumes)
# Override the command with: make exec CMD="pytest -q"
exec:
	$(COMPOSE) exec $(SERVICE) $(CMD)

# `python -m asyncio` opens a REPL with a running event loop so `await` works
# directly on the project's async sessions, repositories, and services.
shell:
	$(COMPOSE) exec $(SERVICE) python -m asyncio

# Run as a module so the repo root stays on sys.path for the `app` imports.
load-customers:
	$(COMPOSE) exec $(SERVICE) python -m scripts.payment.load_customers

# Fires racing payment requests against the running api to exercise idempotency
# and the balance constraint; requires the stack to already be up.
simulate-payments:
	$(COMPOSE) exec $(SERVICE) python -m scripts.payment.simulate_payment

simulate-inventory:
	$(COMPOSE) exec $(SERVICE) python -m scripts.inventory.simulate

simulate-consistency:
	$(COMPOSE2) exec $(SERVICE) python -m scripts.consistency.simulate