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
from concurrent.futures import ThreadPoolExecutor
import threading

logger = logging.getLogger(__name__)

# Initialize the search engine
search_engine = SimpleSearchEngine(index_dir="./wikipedia_index")

# Initialize the graph database (will be used if graph extraction is enabled)
graph_db = None


def index_document(doc_id: str, title: str, content: str, metadata: Dict[str, Any], enable_graph: bool = False, model_name: str = "urchade/gliner_mediumv2.1", device: str = "cpu") -> None:
    """
    Index a document into the search engine and optionally extract graph data.

    Args:
        doc_id: Unique identifier for the document
        title: Document title
        content: Document content/body text
        metadata: Additional metadata (url, categories, etc.)
        enable_graph: Whether to extract and store graph data
        model_name: Model to use for graph extraction
        device: Device to run models on (cpu or cuda)
    """
    # Index in the search engine
    search_engine.feed_document(doc_id, title, content, metadata)

    # Extract and store graph data if enabled
    if enable_graph and graph_db:
        try:
            logger.info(f"Extracting graph data for document: {title}")
            extraction_result = extract_graph_from_document(doc_id, title, content, model_name=model_name, device=device)

            # Store in graph database
            storage_result = graph_db.store_extraction_result(doc_id, title, extraction_result, metadata)

            logger.info(f"Graph extraction complete for '{title}': "
                       f"{storage_result['entities_stored']} entities, "
                       f"{storage_result['relations_stored']} relations stored")
        except Exception as e:
            logger.error(f"Failed to extract graph data for '{title}': {e}")
            # Continue with regular indexing even if graph extraction fails


def process_single_document(article_data: tuple, enable_graph: bool, model_name: str, device: str, language: str) -> bool:
    """
    Process a single document. Thread-safe function for parallel processing.

    Args:
        article_data: Tuple of (index, article) data
        enable_graph: Whether to extract and store graph data
        model_name: Model to use for graph extraction
        device: Device to run models on
        language: Language code

    Returns:
        True if processing succeeded, False otherwise
    """
    i, article = article_data

    try:
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
            return False

        logger.info(f"Processing: {title}")

        # Index the document (with optional graph extraction)
        index_document(doc_id, title, content, metadata, enable_graph, model_name, device)
        return True

    except Exception as e:
        logger.error(f"Error processing document at index {i}: {e}")
        return False


def download_and_index_wikipedia(n_documents: int = 1000, language: str = "en", enable_graph: bool = False, model_name: str = "urchade/gliner_mediumv2.1", device: str = "cpu", parallel_workers: int = 1) -> None:
    """
    Download and process N most popular Wikipedia pages.

    Args:
        n_documents: Number of documents to process
        language: Wikipedia language code (default: "en" for English)
        enable_graph: Whether to extract and store graph data
        model_name: Model to use for graph extraction
        device: Device to run models on (cpu or cuda)
        parallel_workers: Number of parallel workers (default: 1)
    """
    global graph_db

    # Initialize graph database if graph extraction is enabled
    if enable_graph:
        graph_db = GraphDatabase(database="wiki_search")
        if not graph_db.test_connection():
            logger.error("Failed to connect to graph database. Graph extraction will be disabled.")
            enable_graph = False
            graph_db = None
        else:
            logger.info("Graph database connection established. Graph extraction enabled.")

    logger.info(f"Loading Wikipedia dataset for language: {language}")
    logger.info(f"Using {parallel_workers} worker(s) for processing")

    try:
        # Load dataset with streaming to avoid downloading everything
        dataset = load_dataset("wikimedia/wikipedia", "20231101.en", split="train", streaming=True)

        logger.info(f"Starting to process {n_documents} Wikipedia pages...")

        # Collect articles for processing
        articles_to_process = []
        for i, article in enumerate(dataset):
            if len(articles_to_process) >= n_documents:
                break
            articles_to_process.append((i, article))

        processed = 0

        if parallel_workers == 1:
            # Sequential processing
            for article_data in articles_to_process:
                if process_single_document(article_data, enable_graph, model_name, device, language):
                    processed += 1
        else:
            # Parallel processing
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                # Submit all tasks
                futures = [
                    executor.submit(process_single_document, article_data, enable_graph, model_name, device, language)
                    for article_data in articles_to_process
                ]

                # Collect results
                for future in futures:
                    try:
                        if future.result():
                            processed += 1
                    except Exception as e:
                        logger.error(f"Error in parallel processing: {e}")

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
    parser.add_argument(
        "--model",
        type=str,
        default="urchade/gliner_mediumv2.1",
        help="Model to use for graph extraction. Options: \n"
             "- urchade/gliner_mediumv2.1 (default, uses GLiREL)\n"
             "- knowledgator/gliner-x-small (GLiNER only)\n"
             "- knowledgator/gliner-multitask-v1.0 (multitask model)"
    )
    parser.add_argument(
        "--device",
        type=str,
        default="cpu",
        choices=["cpu", "cuda"],
        help="Device to run models on (default: cpu). Use 'cuda' for GPU acceleration."
    )
    parser.add_argument(
        "--parallel",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel workers for processing documents (default: 1). "
             "WARNING: Using >1 with --device cuda may cause GPU memory conflicts."
    )

    args = parser.parse_args()

    download_and_index_wikipedia(
        n_documents=args.num_documents,
        language=args.language,
        enable_graph=args.enable_graph,
        model_name=args.model,
        device=args.device,
        parallel_workers=args.parallel
    )


if __name__ == "__main__":
    main()
