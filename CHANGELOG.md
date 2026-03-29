# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-03-29

### Added

- `src/inbound_planning/vector_search.py` — `VectorSearchManager` class
  - Creates or waits for a Vector Search endpoint (`STANDARD` type)
  - Creates a Delta Sync index on `knowledge_base` with `databricks-gte-large-en` embeddings
  - `sync_index()` triggers a pipeline sync; called as the final step of `process_data_job`
  - `search(query, num_results, filters)` runs similarity search returning `id`, `text`, `warehouse`, `week`, `doc_type`
- `notebooks/test_vector_search.ipynb` — interactive notebook for testing the vector index and RAG pipeline against a live Databricks cluster
  - Section 1: raw similarity search with optional `doc_type` / `warehouse` filters
  - Section 2: `rag_query()` helper that retrieves context and calls `databricks-meta-llama-3-3-70b-instruct`

### Changed

- `resources/process_data_job.yml` replaces `resources/synthetic_data_job.yml` (job renamed to `process_data_job`)
- `resources/deployment_scripts/process_data.py` replaces `generate_synthetic_data.py`; now runs a 3-step pipeline: UC bootstrap → `DataGenerator.run()` → `VectorSearchManager.sync_index()`
- `knowledge_base` Delta table now created with `delta.enableChangeDataFeed = true` (required for Delta Sync index)
- `pyproject.toml`: version field is now static (`version = "0.1.0"`); removed `dynamic = ["version"]` and `[tool.setuptools.dynamic]` to support `uv version --bump`

## [0.0.1] - 2026-03-26

### Added

- `src/inbound_planning/` production package with `config.py` and `data_generator.py`
  - `ProjectConfig` model (Pydantic v2) loaded from `project_config.yml`
  - `DataGenerator` class that builds 10 weeks of synthetic inbound forecast data, inserts scenario events, adds derived features (`utilization`, `status`), generates warehouse and network-level text documents, and writes both `forecast_data` and `knowledge_base` Delta tables
- `project_config.yml` with separate catalog entries per environment (`dev`, `acc`, `prod`)
- `resources/synthetic_data_job.yml` Databricks Asset Bundle job definition
- `resources/deployment_scripts/generate_synthetic_data.py` deployment notebook with idempotent Unity Catalog bootstrap (`CREATE CATALOG/SCHEMA/TABLE IF NOT EXISTS`)
- `databricks.yml` bundle with `dev` (user workspace), `acc`, and `prod` (shared) targets
- `.github/workflows/ci.yml` — runs quality checks, deploys bundle to `acc`, and executes the pipeline on every PR to `main`; also supports `workflow_dispatch`
- `.github/workflows/deploy-prod.yml` — deploys bundle to `prod` on every push to `main`; also supports `workflow_dispatch`

### Changed

- `pyproject.toml`: renamed package from `your_custom_package` to `inbound-planning`; fixed `setuptools.packages.find.include` to `inbound_planning*`
- `pyproject.toml`: moved ruff `select`/`ignore` to `[tool.ruff.lint]` section (resolves deprecation warnings); removed removed rules `ANN101`/`ANN102`; excluded `notebooks/data_generation.ipynb` from ruff (notebook contains `%sql` magic cells that are not valid Python)
- `setup-uv` GitHub Action upgraded from `v5.4.1` to `v7.6.0` in both workflow files

### Fixed

- `IntType` → `IntegerType` in `data_generator.py` (PySpark runtime compatibility)
- `config.py` environment validator now accepts `"prod"` instead of `"prd"`
- Tab indentation in `generate_synthetic_data.py` SQL strings replaced with spaces (W191)
- Long line in `config.py` `system_prompt` default value split to fit 90-char limit (E501)
- Missing `-> None` return type annotations added to test functions in `tests/test_basic.py` (ANN201)
