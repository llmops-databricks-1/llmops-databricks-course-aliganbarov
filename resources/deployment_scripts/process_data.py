# Databricks notebook source
# MAGIC %md
# MAGIC # Data Generation Pipeline
# MAGIC
# MAGIC This notebook generates synthetic inbound planning data, stores it in
# MAGIC Unity Catalog, and syncs the vector search index with the latest knowledge
# MAGIC base documents.
# MAGIC
# MAGIC Pipeline steps:
# MAGIC 1. Bootstrap Unity Catalog objects (idempotent)
# MAGIC 2. Generate synthetic forecast and knowledge base data
# MAGIC 3. Sync vector search index

# COMMAND ----------

from loguru import logger
from pyspark.sql import SparkSession

from inbound_planning.config import get_env, load_config
from inbound_planning.data_generator import DataGenerator
from inbound_planning.vector_search import VectorSearchManager

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

env = get_env(spark)
cfg = load_config("../../project_config.yml", env=env)

logger.info("Configuration loaded:")
logger.info(f"  Environment: {env}")
logger.info(f"  Catalog: {cfg.catalog}")
logger.info(f"  Schema: {cfg.schema}")
logger.info(f"  Weeks generated: {cfg.n_weeks}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Bootstrap Unity Catalog Objects

# COMMAND ----------

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
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """
)

spark.sql(
    f"""
    ALTER TABLE {cfg.catalog}.{cfg.schema}.knowledge_base
    SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Generate And Store Synthetic Historical Data

# COMMAND ----------

generator = DataGenerator(spark=spark, config=cfg)
generator.run()

logger.info("✓ Synthetic data generation complete!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Sync Vector Search Index


# COMMAND ----------

vs_manager = VectorSearchManager(config=cfg)
vs_manager.sync_index()

logger.info("✓ Data generation pipeline complete!")
