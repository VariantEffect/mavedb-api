VENV := .venv/bin

.DEFAULT_GOAL := help

.PHONY: help
help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-12s %s\n", $$1, $$2}'

.PHONY: dev
dev: ## Install deps including local editable variant-annotation
	poetry install --extras server

.PHONY: test
test: ## Run the test suite
	$(VENV)/pytest tests/

.PHONY: lint
lint: ## Check code with ruff
	$(VENV)/ruff check src/ tests/

.PHONY: format
format: ## Format code with ruff
	$(VENV)/ruff format src/ tests/
