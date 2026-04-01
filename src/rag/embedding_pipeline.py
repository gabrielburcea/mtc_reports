"""
RAG Embedding Pipeline - SQL Template and Documentation Embedding
=================================================================
Purpose: Embed SQL templates and documentation into a vector store
for semantic retrieval during query generation.

Key Responsibilities:
1. Load SQL templates from sql/outputs/ and sql/views/
2. Load documentation (README, business rules)
3. Chunk and embed documents
4. Store in FAISS vector store
5. Semantic search functionality
6. Hybrid search (vector + keyword)
"""

import glob as glob_module
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, Field

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------

class Document(BaseModel):
    """A document chunk ready for embedding."""

    doc_id: str
    content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)
    source: str = ""


class SearchResult(BaseModel):
    """A single semantic search result."""

    doc_id: str
    content: str
    score: float
    metadata: Dict[str, Any] = Field(default_factory=dict)
    source: str = ""


# ---------------------------------------------------------------------------
# Embedding Pipeline
# ---------------------------------------------------------------------------

class EmbeddingPipeline:
    """
    Loads SQL templates and documentation, embeds them using OpenAI embeddings,
    and stores in a FAISS index for retrieval.
    """

    def __init__(
        self,
        openai_api_key: Optional[str] = None,
        embedding_model: str = "text-embedding-3-small",
        repo_root: Optional[str] = None,
    ) -> None:
        """
        Initialise the embedding pipeline.

        Args:
            openai_api_key: OpenAI API key (falls back to env var).
            embedding_model: OpenAI embedding model name.
            repo_root: Root of the repository (auto-detected if None).
        """
        self.api_key = openai_api_key or os.getenv("OPENAI_API_KEY")
        self.embedding_model = embedding_model
        self.repo_root = Path(repo_root) if repo_root else Path(__file__).resolve().parents[3]
        self._documents: List[Document] = []
        self._embeddings: Optional[Any] = None  # OpenAIEmbeddings instance
        self._vector_store: Optional[Any] = None  # FAISS instance
        self._is_built = False
        logger.info("EmbeddingPipeline initialised (repo_root=%s)", self.repo_root)

    # ------------------------------------------------------------------
    # Document loading
    # ------------------------------------------------------------------

    def load_sql_files(self) -> List[Document]:
        """
        Load all SQL files from sql/outputs/ and sql/views/.

        Returns:
            List of Document objects.
        """
        docs: List[Document] = []
        patterns = [
            self.repo_root / "sql" / "outputs" / "*.sql",
            self.repo_root / "sql" / "views" / "*.sql",
        ]
        for pattern in patterns:
            for path in glob_module.glob(str(pattern)):
                try:
                    content = Path(path).read_text(encoding="utf-8")
                    name = Path(path).stem
                    source_type = "output" if "outputs" in path else "view"
                    docs.append(
                        Document(
                            doc_id=f"sql_{source_type}_{name}",
                            content=content,
                            metadata={"type": "sql", "source_type": source_type, "name": name},
                            source=path,
                        )
                    )
                    logger.debug("Loaded SQL file: %s", path)
                except Exception as exc:
                    logger.warning("Failed to load SQL file %s: %s", path, exc)
        logger.info("Loaded %d SQL documents", len(docs))
        return docs

    def load_documentation(self) -> List[Document]:
        """
        Load README and any Markdown documentation.

        Returns:
            List of Document objects.
        """
        docs: List[Document] = []
        doc_patterns = [
            self.repo_root / "README.md",
            self.repo_root / "deploy" / "*.md",
        ]
        for pattern in doc_patterns:
            for path in glob_module.glob(str(pattern)):
                try:
                    content = Path(path).read_text(encoding="utf-8")
                    name = Path(path).stem
                    # Chunk large docs into sections
                    chunks = self._chunk_markdown(content, name)
                    docs.extend(chunks)
                    logger.debug("Loaded documentation: %s (%d chunks)", path, len(chunks))
                except Exception as exc:
                    logger.warning("Failed to load doc %s: %s", path, exc)
        logger.info("Loaded %d documentation chunks", len(docs))
        return docs

    def _chunk_markdown(self, content: str, name: str, max_chars: int = 1500) -> List[Document]:
        """Split a Markdown document into section-based chunks."""
        sections = re.split(r"\n#{1,3} ", content)
        chunks = []
        for i, section in enumerate(sections):
            if not section.strip():
                continue
            # Further split if too large
            for j in range(0, len(section), max_chars):
                chunk_text = section[j: j + max_chars].strip()
                if chunk_text:
                    chunks.append(
                        Document(
                            doc_id=f"doc_{name}_{i}_{j // max_chars}",
                            content=chunk_text,
                            metadata={"type": "documentation", "source_name": name},
                            source=name,
                        )
                    )
        return chunks or [Document(doc_id=f"doc_{name}_0", content=content[:max_chars], source=name)]

    # ------------------------------------------------------------------
    # Vector store build / update
    # ------------------------------------------------------------------

    def build(self, force_rebuild: bool = False) -> None:
        """
        Load all documents and build the FAISS vector store.

        Args:
            force_rebuild: If True, rebuild even if already built.
        """
        if self._is_built and not force_rebuild:
            logger.info("Vector store already built. Use force_rebuild=True to rebuild.")
            return

        self._documents = self.load_sql_files() + self.load_documentation()
        if not self._documents:
            logger.warning("No documents found to embed.")
            return

        try:
            from langchain_openai import OpenAIEmbeddings
            from langchain_community.vectorstores import FAISS
            from langchain.schema import Document as LCDocument

            self._embeddings = OpenAIEmbeddings(
                model=self.embedding_model,
                api_key=self.api_key,
            )
            lc_docs = [
                LCDocument(
                    page_content=doc.content,
                    metadata={**doc.metadata, "doc_id": doc.doc_id, "source": doc.source},
                )
                for doc in self._documents
            ]
            self._vector_store = FAISS.from_documents(lc_docs, self._embeddings)
            self._is_built = True
            logger.info("FAISS vector store built with %d documents", len(self._documents))

        except ImportError as exc:
            logger.warning(
                "FAISS/LangChain not available (%s). Falling back to keyword search.", exc
            )
            self._is_built = True  # Mark as built in keyword-only mode

    def save(self, path: str) -> None:
        """Save the FAISS index to disk."""
        if self._vector_store is None:
            raise RuntimeError("Vector store not built. Call build() first.")
        self._vector_store.save_local(path)
        logger.info("Vector store saved to %s", path)

    def load(self, path: str) -> None:
        """Load a previously saved FAISS index from disk."""
        try:
            from langchain_openai import OpenAIEmbeddings
            from langchain_community.vectorstores import FAISS

            self._embeddings = OpenAIEmbeddings(
                model=self.embedding_model, api_key=self.api_key
            )
            self._vector_store = FAISS.load_local(
                path, self._embeddings, allow_dangerous_deserialization=True
            )
            self._is_built = True
            logger.info("Vector store loaded from %s", path)
        except Exception as exc:
            logger.error("Failed to load vector store: %s", exc)
            raise

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def semantic_search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        """
        Perform semantic (vector) search.

        Args:
            query: Natural language query string.
            top_k: Number of results to return.

        Returns:
            List of SearchResult ordered by relevance.
        """
        if not self._is_built:
            self.build()

        if self._vector_store is not None:
            try:
                results_with_scores = self._vector_store.similarity_search_with_score(query, k=top_k)
                return [
                    SearchResult(
                        doc_id=doc.metadata.get("doc_id", ""),
                        content=doc.page_content,
                        score=float(score),
                        metadata=doc.metadata,
                        source=doc.metadata.get("source", ""),
                    )
                    for doc, score in results_with_scores
                ]
            except Exception as exc:
                logger.warning("Vector search failed, falling back to keyword: %s", exc)

        return self._keyword_search(query, top_k)

    def hybrid_search(
        self, query: str, top_k: int = 5, vector_weight: float = 0.7
    ) -> List[SearchResult]:
        """
        Combine vector and keyword search results.

        Args:
            query: Natural language query string.
            top_k: Number of results to return.
            vector_weight: Weight given to vector results (0-1).

        Returns:
            Merged list of SearchResult.
        """
        vector_results = self.semantic_search(query, top_k=top_k * 2)
        keyword_results = self._keyword_search(query, top_k=top_k * 2)

        # Merge by doc_id, combining scores
        merged: Dict[str, Tuple[SearchResult, float]] = {}
        for i, r in enumerate(vector_results):
            rank_score = (top_k * 2 - i) / (top_k * 2)
            merged[r.doc_id] = (r, rank_score * vector_weight)
        for i, r in enumerate(keyword_results):
            rank_score = (top_k * 2 - i) / (top_k * 2)
            if r.doc_id in merged:
                merged[r.doc_id] = (
                    merged[r.doc_id][0],
                    merged[r.doc_id][1] + rank_score * (1 - vector_weight),
                )
            else:
                merged[r.doc_id] = (r, rank_score * (1 - vector_weight))

        sorted_results = sorted(merged.values(), key=lambda x: x[1], reverse=True)
        return [r for r, _ in sorted_results[:top_k]]

    def _keyword_search(self, query: str, top_k: int = 5) -> List[SearchResult]:
        """Fallback keyword-based search over loaded documents."""
        query_terms = query.lower().split()
        scored: List[Tuple[Document, float]] = []
        for doc in self._documents:
            content_lower = doc.content.lower()
            score = sum(content_lower.count(term) for term in query_terms)
            if score > 0:
                scored.append((doc, float(score)))
        scored.sort(key=lambda x: x[1], reverse=True)
        return [
            SearchResult(
                doc_id=doc.doc_id,
                content=doc.content[:500],
                score=score,
                metadata=doc.metadata,
                source=doc.source,
            )
            for doc, score in scored[:top_k]
        ]

    @property
    def document_count(self) -> int:
        """Return number of loaded documents."""
        return len(self._documents)


# ---------------------------------------------------------------------------
# Example Usage
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    pipeline = EmbeddingPipeline()
    pipeline.build()
    print(f"Documents loaded: {pipeline.document_count}")

    results = pipeline.hybrid_search("MTC national pupil characteristics gender gap", top_k=3)
    print(f"\nTop {len(results)} results:")
    for r in results:
        print(f"  [{r.score:.2f}] {r.doc_id}: {r.content[:100]}...")
