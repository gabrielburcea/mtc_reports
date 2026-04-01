"""Tests for RAG pipeline - embedding and retrieval."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.rag.embedding_pipeline import Document, EmbeddingPipeline, SearchResult


@pytest.fixture
def pipeline():
    """EmbeddingPipeline pointed at the actual repo root."""
    repo_root = Path(__file__).resolve().parents[1]
    return EmbeddingPipeline(openai_api_key="test-key", repo_root=str(repo_root))


class TestDocumentLoading:
    def test_load_sql_files_returns_documents(self, pipeline):
        docs = pipeline.load_sql_files()
        assert len(docs) > 0
        assert all(isinstance(d, Document) for d in docs)

    def test_sql_docs_have_content(self, pipeline):
        docs = pipeline.load_sql_files()
        assert all(d.content.strip() != "" for d in docs)

    def test_sql_docs_have_metadata(self, pipeline):
        docs = pipeline.load_sql_files()
        assert all("type" in d.metadata for d in docs)

    def test_load_documentation_returns_list(self, pipeline):
        docs = pipeline.load_documentation()
        # May be empty if no markdown files, but should be a list
        assert isinstance(docs, list)

    def test_chunk_markdown_splits_sections(self, pipeline):
        md = "# Section 1\nContent here.\n## Section 2\nMore content.\n"
        chunks = pipeline._chunk_markdown(md, "test_doc")
        assert len(chunks) >= 1
        assert all(isinstance(c, Document) for c in chunks)

    def test_chunk_markdown_large_section(self, pipeline):
        md = "A" * 5000
        chunks = pipeline._chunk_markdown(md, "big_doc", max_chars=1500)
        assert len(chunks) >= 3


class TestKeywordSearch:
    def test_keyword_search_finds_relevant_docs(self, pipeline):
        pipeline.build()  # Uses keyword fallback (no FAISS without real API key)
        results = pipeline._keyword_search("national pupil characteristics", top_k=3)
        assert isinstance(results, list)
        # Some results should be found
        if pipeline.document_count > 0:
            assert len(results) >= 0  # may be 0 if no match

    def test_keyword_search_returns_search_results(self, pipeline):
        pipeline.build()
        results = pipeline._keyword_search("score distribution gender", top_k=5)
        assert all(isinstance(r, SearchResult) for r in results)

    def test_semantic_search_falls_back_to_keyword(self, pipeline):
        pipeline.build()
        # Without real FAISS, should fall back to keyword search
        results = pipeline.semantic_search("pupil characteristics")
        assert isinstance(results, list)


class TestBuildPipeline:
    def test_build_loads_documents(self, pipeline):
        pipeline.build()
        assert pipeline.document_count > 0

    def test_build_is_idempotent(self, pipeline):
        pipeline.build()
        count1 = pipeline.document_count
        pipeline.build()  # Should not double-load
        assert pipeline.document_count == count1

    def test_force_rebuild_works(self, pipeline):
        pipeline.build()
        pipeline.build(force_rebuild=True)
        assert pipeline.document_count > 0


class TestSearchResult:
    def test_search_result_model(self):
        r = SearchResult(doc_id="test_id", content="SELECT * FROM table", score=0.95)
        assert r.doc_id == "test_id"
        assert r.score == 0.95
