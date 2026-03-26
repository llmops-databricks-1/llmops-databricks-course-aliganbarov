<h1 align="center">
Inbound Planning — LLMOps on Databricks
</h1>

A RAG-based forecasting assistant for e-commerce warehouse inbound planning, built as part of the LLMOps on Databricks course. The system generates synthetic historical inbound data, stores it in Unity Catalog, and exposes it to an LLM agent for natural-language analysis.

## Project structure

```
src/inbound_planning/       # Production package
  config.py                 # ProjectConfig (Pydantic), load_config(), get_env()
  data_generator.py         # DataGenerator: synthetic forecast + knowledge base

resources/
  synthetic_data_job.yml    # Databricks Asset Bundle job definition
  deployment_scripts/
    generate_synthetic_data.py  # Notebook run by the bundle job

notebooks/
  data_generation.ipynb     # Exploratory prototype (not linted)

.github/workflows/
  ci.yml                    # PR → quality checks + deploy to acc
  deploy-prod.yml           # Merge to main → deploy to prod

project_config.yml          # Per-environment catalog/schema/endpoint config
databricks.yml              # Bundle definition (dev / acc / prod targets)
```

## Unity Catalog layout

| Environment | Catalog                  |
|-------------|--------------------------|
| dev         | `llmops_forecasting_dev` |
| acc         | `llmops_forecasting_acc` |
| prod        | `llmops_forecasting_prod`|

Schema: `inbound_planning`
Tables: `forecast_data`, `knowledge_base`

## Set up your environment

Requires Python 3.12 (Databricks Serverless Environment 4) and [uv](https://docs.astral.sh/uv/getting-started/installation/).

```bash
uv sync --extra dev
```

## Local development

Deploy and run the data generation job against the `dev` catalog:

```bash
databricks bundle deploy -t dev
databricks bundle run generate_synthetic_data -t dev
```

> Never deploy to `acc` or `prod` locally — those environments are managed exclusively by CI.

## CI/CD

| Event              | Workflow           | Action                          |
|--------------------|--------------------|---------------------------------|
| PR to `main`       | `ci.yml`           | Lint → pytest → deploy to `acc` |
| Merge to `main`    | `deploy-prod.yml`  | Build wheel → deploy to `prod`  |
| Manual trigger     | both               | `workflow_dispatch` supported   |

Required GitHub Actions secrets:

| Secret                        | Used by           |
|-------------------------------|-------------------|
| `DATABRICKS_ACC_CLIENT_ID`    | `ci.yml`          |
| `DATABRICKS_ACC_CLIENT_SECRET`| `ci.yml`          |
| `DATABRICKS_PROD_CLIENT_ID`   | `deploy-prod.yml` |
| `DATABRICKS_PROD_CLIENT_SECRET`| `deploy-prod.yml`|

## Running pre-commit

```bash
pre-commit run --all-files
```
