"""Vector search management for the inbound planning knowledge base."""

import datetime
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.vector_search.client import VectorSearchClient
from loguru import logger

from inbound_planning.config import ProjectConfig


class VectorSearchManager:
    """Manages vector search endpoint and index for the inbound planning knowledge base."""

    def __init__(
        self,
        config: ProjectConfig,
        endpoint_name: str | None = None,
        embedding_model: str | None = None,
        usage_policy_id: str | None = None,
    ) -> None:
        """Initialize VectorSearchManager.

        Args:
            config: ProjectConfig object.
            endpoint_name: Vector search endpoint name (uses config if None).
            embedding_model: Embedding model endpoint name (uses config if None).
            usage_policy_id: Optional usage policy ID for the endpoint.
        """
        self.config = config
        self.endpoint_name = endpoint_name or config.vector_search_endpoint
        self.embedding_model = embedding_model or config.embedding_endpoint
        self.catalog = config.catalog
        self.schema = config.schema
        self.usage_policy_id = usage_policy_id

        w = WorkspaceClient()
        self.client = VectorSearchClient(
            workspace_url=w.config.host,
            personal_access_token=w.tokens.create(lifetime_seconds=1200).token_value,
        )
        self.index_name = f"{self.catalog}.{self.schema}.knowledge_base_index"
        self.source_table = f"{self.catalog}.{self.schema}.knowledge_base"

    def create_endpoint_if_not_exists(self) -> None:
        """Create the vector search endpoint if it does not already exist, and wait until ready."""
        endpoints_response = self.client.list_endpoints()
        endpoints = (
            endpoints_response.get("endpoints", [])
            if isinstance(endpoints_response, dict)
            else []
        )
        endpoint_exists = any(
            (ep.get("name") if isinstance(ep, dict) else getattr(ep, "name", None))
            == self.endpoint_name
            for ep in endpoints
        )

        if not endpoint_exists:
            logger.info(f"Creating vector search endpoint: {self.endpoint_name}")
            self.client.create_endpoint_and_wait(
                name=self.endpoint_name,
                endpoint_type="STANDARD",
                usage_policy_id=self.usage_policy_id,
            )
            logger.info(f"✓ Vector search endpoint created: {self.endpoint_name}")
        else:
            logger.info(f"Vector search endpoint exists: {self.endpoint_name}, waiting until ready...")
            self.client.wait_for_endpoint(
                name=self.endpoint_name,
                timeout=datetime.timedelta(seconds=600),
            )
            logger.info(f"✓ Vector search endpoint ready: {self.endpoint_name}")

    def create_or_get_index(self) -> Any:
        """Create the Delta Sync index if it does not exist, or return the existing one.

        Returns:
            Vector search index object.
        """
        self.create_endpoint_if_not_exists()

        try:
            index = self.client.get_index(index_name=self.index_name)
            logger.info(f"✓ Vector search index exists: {self.index_name}")
            return index
        except Exception:
            logger.info(f"Index {self.index_name} not found, creating it")

        try:
            index = self.client.create_delta_sync_index(
                endpoint_name=self.endpoint_name,
                source_table_name=self.source_table,
                index_name=self.index_name,
                pipeline_type="TRIGGERED",
                primary_key="id",
                embedding_source_column="text",
                embedding_model_endpoint_name=self.embedding_model,
                usage_policy_id=self.usage_policy_id,
            )
            logger.info(f"✓ Vector search index created: {self.index_name}")
            return index
        except Exception as e:
            if "RESOURCE_ALREADY_EXISTS" not in str(e):
                raise
            logger.info(f"✓ Vector search index exists: {self.index_name}")
            return self.client.get_index(index_name=self.index_name)

    def sync_index(self) -> None:
        """Trigger a sync of the vector search index with the source Delta table."""
        index = self.create_or_get_index()
        logger.info(f"Syncing vector search index: {self.index_name}")
        index.sync()
        logger.info("✓ Index sync triggered")

    def search(
        self,
        query: str,
        num_results: int = 5,
        filters: dict | None = None,
    ) -> dict:
        """Search the knowledge base index by similarity.

        Args:
            query: Natural-language query text.
            num_results: Number of results to return.
            filters: Optional column filters, e.g. ``{"week": 12}`` or
                ``{"doc_type": "network_summary"}``.

        Returns:
            Search results dictionary from the Vector Search API.
        """
        index = self.client.get_index(index_name=self.index_name)
        results = index.similarity_search(
            query_text=query,
            columns=["id", "text", "warehouse", "week", "doc_type"],
            num_results=num_results,
            filters=filters,
        )
        return results
