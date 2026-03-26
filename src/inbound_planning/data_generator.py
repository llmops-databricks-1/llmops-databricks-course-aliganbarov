"""
Synthetic data generation for inbound forecast interpretation.

Pipeline:
    DataGenerator.run()
        ↓ _build_capacities()   → capacity per warehouse
        ↓ _build_forecast()     → base forecast with seasonality + noise
        ↓ _inject_scenarios()   → realistic edge-case events
        ↓ _add_features()       → utilization, change_pct, status labels
        ↓ _generate_warehouse_docs() → natural-language warehouse documents
        ↓ _generate_network_docs()   → weekly network-level summaries
        ↓ _write_forecast()     → Delta table: forecast_data
        ↓ _write_knowledge_base() → Delta table: knowledge_base
"""

import datetime
import uuid

import numpy as np
import pandas as pd
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from inbound_planning.config import ProjectConfig

_WAREHOUSES = [
    "Berlin",
    "Hamburg",
    "Munich",
    "Cologne",
    "Frankfurt",
    "Stuttgart",
    "Düsseldorf",
    "Leipzig",
    "Dortmund",
    "Essen",
    "Bremen",
    "Dresden",
    "Hannover",
    "Nuremberg",
    "Duisburg",
    "Bochum",
    "Wuppertal",
    "Bielefeld",
    "Bonn",
    "Münster",
    "Karlsruhe",
    "Mannheim",
    "Augsburg",
    "Wiesbaden",
    "Gelsenkirchen",
    "Mönchengladbach",
    "Braunschweig",
    "Chemnitz",
    "Kiel",
    "Aachen",
]

_LARGE = {"Berlin", "Hamburg", "Munich", "Frankfurt"}
_MEDIUM = {"Cologne", "Stuttgart", "Düsseldorf"}

_STATUS_TEXT = {
    "critical_overflow": "This warehouse is significantly over capacity.",
    "overflow": "This warehouse exceeds its capacity.",
    "high": "This warehouse is operating close to capacity.",
    "normal": "This warehouse is operating within capacity limits.",
}


class DataGenerator:
    """
    Generates static historical synthetic inbound forecast data and
    writes it to Unity Catalog Delta tables.

    Tables written:
    - ``<catalog>.<schema>.forecast_data``  — structured ground-truth dataset
    - ``<catalog>.<schema>.knowledge_base`` — LLM-ready natural-language documents
    """

    def __init__(
        self,
        spark: SparkSession,
        config: ProjectConfig,
        random_seed: int = 42,
    ) -> None:
        """
        Args:
            spark: Active SparkSession.
            config: ProjectConfig with catalog, schema, n_weeks.
            random_seed: Seed for reproducibility.
        """
        self.spark = spark
        self.config = config
        self.catalog = config.catalog
        self.schema = config.schema
        self.n_weeks = config.n_weeks
        self.random_seed = random_seed

        current_iso_week = datetime.date.today().isocalendar().week
        self.end_week = current_iso_week
        self.weeks = list(
            range(current_iso_week - self.n_weeks + 1, current_iso_week + 1)
        )

        self.forecast_table = f"{self.catalog}.{self.schema}.forecast_data"
        self.kb_table = f"{self.catalog}.{self.schema}.knowledge_base"

        logger.info(
            f"DataGenerator initialised | weeks {self.weeks[0]}–{self.weeks[-1]} "
            f"| tables: {self.forecast_table}, {self.kb_table}"
        )

    # ------------------------------------------------------------------
    # Data generation
    # ------------------------------------------------------------------

    def _build_capacities(self) -> pd.DataFrame:
        """Assign realistic capacities to each warehouse."""
        rng = np.random.default_rng(self.random_seed)
        rows = []
        for city in _WAREHOUSES:
            if city in _LARGE:
                capacity = int(rng.integers(110_000, 140_001))
            elif city in _MEDIUM:
                capacity = int(rng.integers(90_000, 110_001))
            else:
                capacity = int(rng.integers(70_000, 95_001))
            rows.append({"warehouse": city, "capacity": capacity})
        return pd.DataFrame(rows)

    def _build_forecast(self, capacity_df: pd.DataFrame) -> pd.DataFrame:
        """Generate base weekly forecast with seasonality and noise."""
        rng = np.random.default_rng(self.random_seed)
        rows = []
        for _, row in capacity_df.iterrows():
            city = row["warehouse"]
            base = int(rng.integers(60_000, 100_001))
            for week in self.weeks:
                seasonality = 15_000 * np.sin(2 * np.pi * week / 52)
                noise = rng.normal(0, 8_000)
                forecast = int(max(20_000, base + seasonality + noise))
                rows.append({"week": week, "warehouse": city, "forecast": forecast})
        return pd.DataFrame(rows)

    def _inject_scenarios(self, df: pd.DataFrame) -> pd.DataFrame:
        """Inject realistic edge-case scenarios at the most recent weeks."""
        df = df.copy()
        peak_week = self.end_week
        spike_week = self.end_week - 1

        # Overflow cluster at peak week
        for city in ["Berlin", "Leipzig", "Dresden"]:
            mask = (df["warehouse"] == city) & (df["week"] == peak_week)
            df.loc[mask, "forecast"] = (df.loc[mask, "forecast"] * 1.5).astype(int)

        # Underutilised warehouses at peak week
        for city in ["Bremen", "Kiel", "Augsburg"]:
            mask = (df["warehouse"] == city) & (df["week"] == peak_week)
            df.loc[mask, "forecast"] = (df.loc[mask, "forecast"] * 0.6).astype(int)

        # Demand spike one week before peak
        mask = (df["warehouse"] == "Berlin") & (df["week"] == spike_week)
        df.loc[mask, "forecast"] = (df.loc[mask, "forecast"] * 1.4).astype(int)

        # Near-capacity warehouses at peak week
        for city in ["Frankfurt", "Cologne"]:
            mask = (df["warehouse"] == city) & (df["week"] == peak_week)
            df.loc[mask, "forecast"] = (df.loc[mask, "forecast"] * 1.1).astype(int)

        return df

    def _add_features(self, df: pd.DataFrame, capacity_df: pd.DataFrame) -> pd.DataFrame:
        """Add utilization, week-over-week change, and status label."""
        df = df.merge(capacity_df, on="warehouse").sort_values(["warehouse", "week"])
        df["prev_forecast"] = df.groupby("warehouse")["forecast"].shift(1)
        df["change_pct"] = (df["forecast"] - df["prev_forecast"]) / df["prev_forecast"]
        df["utilization"] = df["forecast"] / df["capacity"]
        df["status"] = df["utilization"].apply(self._classify)
        return df

    @staticmethod
    def _classify(util: float) -> str:
        if util > 1.1:
            return "critical_overflow"
        if util > 1.0:
            return "overflow"
        if util > 0.9:
            return "high"
        return "normal"

    # ------------------------------------------------------------------
    # Document generation
    # ------------------------------------------------------------------

    def _generate_warehouse_doc(self, row: pd.Series) -> str:
        change_text = ""
        if pd.notna(row["change_pct"]):
            if row["change_pct"] > 0.2:
                change_text = (
                    "There is a strong increase in inbound compared to last week."
                )
            elif row["change_pct"] < -0.2:
                change_text = (
                    "There is a significant decrease in inbound compared to last week."
                )
            else:
                change_text = "Inbound volume is relatively stable compared to last week."

        return (
            f"Week {int(row['week'])} - Warehouse {row['warehouse']}\n\n"
            f"Forecasted inbound volume: {row['forecast']:,} units.\n"
            f"Capacity: {row['capacity']:,} units.\n"
            f"Utilization: {row['utilization']:.0%}.\n\n"
            f"{_STATUS_TEXT[row['status']]}\n\n"
            f"{change_text}"
        ).strip()

    def _generate_network_doc(self, df_week: pd.DataFrame) -> str:
        week = int(df_week["week"].iloc[0])
        over = df_week[df_week["utilization"] > 1.0]["warehouse"].tolist()
        high = df_week[(df_week["utilization"] > 0.9) & (df_week["utilization"] <= 1.0)][
            "warehouse"
        ].tolist()
        under = df_week[df_week["utilization"] < 0.7]["warehouse"].tolist()

        return (
            f"Week {week} - Network Summary\n\n"
            f"Warehouses over capacity:\n"
            f"{', '.join(over) if over else 'None'}\n\n"
            f"Warehouses near capacity:\n"
            f"{', '.join(high) if high else 'None'}\n\n"
            f"Underutilised warehouses:\n"
            f"{', '.join(under) if under else 'None'}"
        )

    def generate(self) -> tuple[pd.DataFrame, list[dict]]:
        """Generate synthetic forecast data and knowledge base documents.

        Returns:
            Tuple of (forecast_df, network_docs_list).
        """
        logger.info("Generating capacity data...")
        capacity_df = self._build_capacities()

        logger.info("Generating forecast data...")
        df = self._build_forecast(capacity_df)
        df = self._inject_scenarios(df)
        df = self._add_features(df, capacity_df)

        logger.info("Generating warehouse documents...")
        df["document"] = df.apply(self._generate_warehouse_doc, axis=1)

        logger.info("Generating network summary documents...")
        network_docs = [
            {
                "id": str(uuid.uuid4()),
                "text": self._generate_network_doc(group),
                "week": int(week),
                "warehouse": None,
                "doc_type": "network_summary",
            }
            for week, group in df.groupby("week")
        ]

        logger.info(
            f"Generated {len(df)} warehouse records and "
            f"{len(network_docs)} network summary documents."
        )
        return df, network_docs

    # ------------------------------------------------------------------
    # Delta writes
    # ------------------------------------------------------------------

    def _write_forecast(self, df: pd.DataFrame) -> None:
        """Write forecast data to Delta table (overwrite)."""
        from pyspark.sql.types import (
            DoubleType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )

        schema = StructType(
            [
                StructField("week", IntegerType(), False),
                StructField("warehouse", StringType(), False),
                StructField("forecast", IntegerType(), False),
                StructField("capacity", IntegerType(), False),
                StructField("prev_forecast", IntegerType(), True),
                StructField("change_pct", DoubleType(), True),
                StructField("utilization", DoubleType(), False),
                StructField("status", StringType(), False),
            ]
        )

        spark_df = self.spark.createDataFrame(
            df[
                [
                    "week",
                    "warehouse",
                    "forecast",
                    "capacity",
                    "prev_forecast",
                    "change_pct",
                    "utilization",
                    "status",
                ]
            ],
            schema=schema,
        )

        (
            spark_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.forecast_table)
        )
        logger.info(f"✓ Wrote {spark_df.count()} rows to {self.forecast_table}")

    def _write_knowledge_base(self, df: pd.DataFrame, network_docs: list[dict]) -> None:
        """Write knowledge base documents to Delta table (overwrite)."""
        warehouse_docs = [
            {
                "id": str(uuid.uuid4()),
                "text": row["document"],
                "warehouse": row["warehouse"],
                "week": int(row["week"]),
                "doc_type": "warehouse",
            }
            for _, row in df.iterrows()
        ]

        all_docs = warehouse_docs + network_docs
        kb_pd = pd.DataFrame(all_docs)
        kb_pd["week"] = kb_pd["week"].astype("Int64")

        spark_kb = self.spark.createDataFrame(kb_pd).select(
            F.col("id").cast("string"),
            F.col("week").cast("int").alias("week"),
            F.col("warehouse").cast("string"),
            F.col("doc_type").cast("string"),
            F.col("text").cast("string"),
        )

        (
            spark_kb.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(self.kb_table)
        )
        logger.info(f"✓ Wrote {spark_kb.count()} documents to {self.kb_table}")

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Run the full data generation and storage pipeline."""
        logger.info("=== Starting data generation pipeline ===")
        df, network_docs = self.generate()
        self._write_forecast(df)
        self._write_knowledge_base(df, network_docs)
        logger.info("=== Data generation pipeline complete ===")
