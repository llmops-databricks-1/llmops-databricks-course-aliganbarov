# Databricks notebook source
# MAGIC %md
# MAGIC # Synthetic Data Generation Pipeline
# MAGIC
# MAGIC This notebook generates static historical synthetic inbound planning data
# MAGIC and stores both structured and document-oriented outputs in Unity Catalog.

# COMMAND ----------

from loguru import logger
from pyspark.sql import SparkSession

from inbound_planning.config import get_env, load_config
from inbound_planning.data_generator import DataGenerator

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

env = get_env(spark)
cfg = load_config("../../project_config.yml", env=env)

logger.info("Configuration loaded:")
logger.info(f"  Environment: {env}")
logger.info(f"  Catalog: {cfg.catalog}")
logger.info(f"  Schema: {cfg.schema}")
logger.info(f"  Weeks generated: {cfg.n_weeks}")

# Ensure Unity Catalog objects exist for first-time runs.
spark.sql(f"CREATE CATALOG IF NOT EXISTS {cfg.catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {cfg.catalog}.{cfg.schema}")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {cfg.catalog}.{cfg.schema}.forecast_data (
        week INT,
        warehouse STRING,
        forecast INT,
        capacity INT,
        prev_forecast INT,
        change_pct DOUBLE,
        utilization DOUBLE,
        status STRING
    )
    USING DELTA
    """
)

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {cfg.catalog}.{cfg.schema}.knowledge_base (
        id STRING,
        week INT,
        warehouse STRING,
        doc_type STRING,
        text STRING
    )
    USING DELTA
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate And Store Synthetic Historical Data

# COMMAND ----------

generator = DataGenerator(spark=spark, config=cfg)
generator.run()

logger.info("✓ Synthetic data generation complete!")
