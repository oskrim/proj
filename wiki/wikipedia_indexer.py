#!/usr/bin/env python3
"""
Download and index popular Wikipedia pages into a search engine.
Uses the Hugging Face datasets library to access Wikipedia data.
"""

import argparse
from typing import Dict, Any
from datasets import load_dataset
import logging
import logging_config  # Centralized logging configuration
from simple_search_engine import SimpleSearchEngine
from graph_extractor import extract_graph_from_document
from graph_database import GraphDatabase

logger = logging.getLogger(__name__)

# Initialize the search engine
search_engine = SimpleSearchEngine(index_dir="./wikipedia_index")

# Initialize the graph database (will be used if graph extraction is enabled)
graph_db = None


def index_document(doc_id: str, title: str, content: str, metadata: Dict[str, Any], enable_graph: bool = False) -> None:
    """
    Index a document into the search engine and optionally extract graph data.

    Args:
        doc_id: Unique identifier for the document
        title: Document title
        content: Document content/body text
        metadata: Additional metadata (url, categories, etc.)
        enable_graph: Whether to extract and store graph data
    """
    # Index in the search engine
    search_engine.feed_document(doc_id, title, content, metadata)
    
    # Extract and store graph data if enabled
    if enable_graph and graph_db:
        try:
            logger.info(f"Extracting graph data for document: {title}")
            extraction_result = extract_graph_from_document(doc_id, title, content)
            
            # Store in graph database
            storage_result = graph_db.store_extraction_result(doc_id, title, extraction_result, metadata)
            
            logger.info(f"Graph extraction complete for '{title}': "
                       f"{storage_result['entities_stored']} entities, "
                       f"{storage_result['relations_stored']} relations stored")
        except Exception as e:
            logger.error(f"Failed to extract graph data for '{title}': {e}")
            # Continue with regular indexing even if graph extraction fails


def download_and_index_wikipedia(n_documents: int = 1000, language: str = "en", enable_graph: bool = False) -> None:
    """
    Download and process N most popular Wikipedia pages.

    Args:
        n_documents: Number of documents to process
        language: Wikipedia language code (default: "en" for English)
        enable_graph: Whether to extract and store graph data
    """
    global graph_db
    
    # Initialize graph database if graph extraction is enabled
    if enable_graph:
        graph_db = GraphDatabase()
        if not graph_db.test_connection():
            logger.error("Failed to connect to graph database. Graph extraction will be disabled.")
            enable_graph = False
            graph_db = None
        else:
            logger.info("Graph database connection established. Graph extraction enabled.")
    logger.info(f"Loading Wikipedia dataset for language: {language}")

    try:
        # Load dataset with streaming to avoid downloading everything
        dataset = load_dataset("wikimedia/wikipedia", "20231101.en", split="train", streaming=True)

        logger.info(f"Starting to process {n_documents} Wikipedia pages...")

        processed = 0
        for i, article in enumerate(dataset):
            if processed >= n_documents:
                break

            # Extract article data
            doc_id = article.get('id', str(i))
            title = article.get('title', '')
            content = article.get('text', '')
            url = article.get('url', '')

            # Prepare metadata
            metadata = {
                'url': url,
                'language': language,
                'source': 'wikipedia',
            }

            # Skip empty articles
            if not title or not content:
                logger.warning(f"Skipping empty article at index {i}")
                continue

            logger.info(f"Processing [{processed + 1}/{n_documents}]: {title}")

            try:
                # Index the document (with optional graph extraction)
                index_document(doc_id, title, content, metadata, enable_graph)
                processed += 1
            except Exception as e:
                logger.error(f"Error indexing document '{title}': {e}")
                continue

        logger.info(f"Successfully processed {processed} documents")

    except Exception as e:
        logger.error(f"Error loading dataset: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(description="Download and index Wikipedia pages")
    parser.add_argument(
        "-n", "--num-documents",
        type=int,
        default=100,
        help="Number of Wikipedia pages to download and index (default: 100)"
    )
    parser.add_argument(
        "-l", "--language",
        type=str,
        default="en",
        help="Wikipedia language code (default: en)"
    )
    parser.add_argument(
        "--enable-graph",
        action="store_true",
        help="Enable graph extraction using GLiNER/GLiREL and store in PostgreSQL"
    )

    args = parser.parse_args()

    download_and_index_wikipedia(
        n_documents=args.num_documents,
        language=args.language,
        enable_graph=args.enable_graph
    )


if __name__ == "__main__":
    main()
