"""
Vector Store - Abstraction layer over FAISS / Databricks Vector Search
======================================================================
Purpose: Provide a consistent interface for storing and retrieving
document embeddings regardless of the backend.
"""

import logging
import os
from typing import List, Optional

from src.rag.embedding_pipeline import EmbeddingPipeline, SearchResult

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class VectorStore:
    """
    High-level vector store that wraps EmbeddingPipeline and adds
    persistence and update capabilities.
    """

    def __init__(
        self,
        store_path: Optional[str] = None,
        openai_api_key: Optional[str] = None,
    ) -> None:
        """
        Initialise the vector store.

        Args:
            store_path: Path to persist/load the FAISS index.
            openai_api_key: OpenAI API key.
        """
        self.store_path = store_path or os.getenv("VECTOR_STORE_PATH", "vector_store_index")
        self._pipeline = EmbeddingPipeline(openai_api_key=openai_api_key)
        logger.info("VectorStore initialised (store_path=%s)", self.store_path)

    def setup(self, force_rebuild: bool = False) -> None:
        """
        Build or load the vector store.

        Args:
            force_rebuild: If True, rebuild from source files.
        """
        if not force_rebuild and os.path.exists(self.store_path):
            try:
                self._pipeline.load(self.store_path)
                logger.info("Loaded existing vector store from %s", self.store_path)
                return
            except Exception as exc:
                logger.warning("Could not load existing store (%s), rebuilding.", exc)

        self._pipeline.build(force_rebuild=True)
        if self._pipeline._vector_store is not None:
            try:
                self._pipeline.save(self.store_path)
                logger.info("Vector store saved to %s", self.store_path)
            except Exception as exc:
                logger.warning("Could not save vector store: %s", exc)

    def search(self, query: str, top_k: int = 5, hybrid: bool = True) -> List[SearchResult]:
        """
        Search the vector store.

        Args:
            query: Natural language query.
            top_k: Number of results.
            hybrid: Use hybrid search if True, pure vector if False.

        Returns:
            List of SearchResult.
        """
        if not self._pipeline._is_built:
            self.setup()

        if hybrid:
            return self._pipeline.hybrid_search(query, top_k=top_k)
        return self._pipeline.semantic_search(query, top_k=top_k)

    def update(self) -> None:
        """Rebuild the vector store from source files."""
        logger.info("Updating vector store from source files...")
        self._pipeline.build(force_rebuild=True)
        if self._pipeline._vector_store is not None:
            self._pipeline.save(self.store_path)

    @property
    def document_count(self) -> int:
        """Number of documents in the store."""
        return self._pipeline.document_count


if __name__ == "__main__":
    store = VectorStore()
    store.setup()
    results = store.search("score distribution by gender MTC 2024")
    for r in results:
        print(f"[{r.score:.3f}] {r.doc_id}")
