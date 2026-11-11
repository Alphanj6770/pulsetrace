.PHONY: install test test-fast lint typecheck check bench run demo

install:
	uv sync

test:
	uv run pytest

test-fast:
	uv run pytest -m "not e2e"

lint:
	uv run ruff check .
	uv run ruff format --check .

typecheck:
	uv run mypy

check: lint typecheck test

bench:
	uv run python benchmarks/overhead.py
	uv run python benchmarks/backpressure.py

run:
	scripts/run_local.sh

demo:
	scripts/demo.sh
