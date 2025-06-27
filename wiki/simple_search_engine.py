#!/usr/bin/env python3
"""
Simple file-based search engine implementation.
Stores documents as files and searches through them using text matching.
"""

import os
import json
import re
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging

logger = logging.getLogger(__name__)


class SimpleSearchEngine:
    """A simple file-based search engine that stores documents and allows text queries."""
    
    def __init__(self, index_dir: str = "./search_index"):
        """
        Initialize the search engine with a directory for storing indexed documents.
        
        Args:
            index_dir: Directory path where indexed documents will be stored
        """
        self.index_dir = Path(index_dir)
        self.index_dir.mkdir(parents=True, exist_ok=True)
        self.metadata_file = self.index_dir / "metadata.json"
        self._load_metadata()
    
    def _load_metadata(self) -> None:
        """Load or initialize the metadata file that tracks all documents."""
        if self.metadata_file.exists():
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                self.metadata = json.load(f)
        else:
            self.metadata = {}
            self._save_metadata()
    
    def _save_metadata(self) -> None:
        """Save the metadata to disk."""
        with open(self.metadata_file, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, indent=2, ensure_ascii=False)
    
    def _sanitize_filename(self, doc_id: str) -> str:
        """Convert document ID to a safe filename."""
        # Replace problematic characters with underscores
        safe_id = re.sub(r'[<>:"/\\|?*]', '_', str(doc_id))
        return safe_id[:200]  # Limit length
    
    def feed_document(self, doc_id: str, title: str, content: str, metadata: Dict[str, Any] = None) -> None:
        """
        Index a document by storing it as a file.
        
        Args:
            doc_id: Unique identifier for the document
            title: Document title
            content: Document content/body text
            metadata: Additional metadata (optional)
        """
        if not doc_id:
            raise ValueError("Document ID cannot be empty")
        
        # Create a safe filename
        safe_filename = self._sanitize_filename(doc_id)
        doc_path = self.index_dir / f"{safe_filename}.txt"
        
        # Combine title and content for the file
        full_content = f"TITLE: {title}\n{'='*50}\n{content}"
        
        # Write document to file
        with open(doc_path, 'w', encoding='utf-8') as f:
            f.write(full_content)
        
        # Update metadata
        self.metadata[doc_id] = {
            'filename': f"{safe_filename}.txt",
            'title': title,
            'metadata': metadata or {},
            'content_length': len(content)
        }
        self._save_metadata()
        
        logger.info(f"Indexed document: {title} (ID: {doc_id})")
    
    def query(self, query_string: str, max_results: int = 10) -> List[Dict[str, Any]]:
        """
        Search for documents containing the query string.
        
        Args:
            query_string: The search query
            max_results: Maximum number of results to return
            
        Returns:
            List of matching documents with their metadata and snippets
        """
        if not query_string:
            return []
        
        results = []
        query_lower = query_string.lower()
        
        # Search through all indexed documents
        for doc_id, doc_meta in self.metadata.items():
            doc_path = self.index_dir / doc_meta['filename']
            
            if not doc_path.exists():
                logger.warning(f"Document file missing: {doc_path}")
                continue
            
            try:
                with open(doc_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                    content_lower = content.lower()
                    
                    # Check if query appears in the document
                    if query_lower in content_lower:
                        # Find snippet around the first match
                        match_pos = content_lower.find(query_lower)
                        snippet_start = max(0, match_pos - 100)
                        snippet_end = min(len(content), match_pos + len(query_string) + 100)
                        snippet = content[snippet_start:snippet_end]
                        
                        # Clean up snippet
                        if snippet_start > 0:
                            snippet = "..." + snippet
                        if snippet_end < len(content):
                            snippet = snippet + "..."
                        
                        # Count occurrences for relevance
                        occurrences = content_lower.count(query_lower)
                        
                        results.append({
                            'doc_id': doc_id,
                            'title': doc_meta['title'],
                            'snippet': snippet.strip(),
                            'occurrences': occurrences,
                            'metadata': doc_meta.get('metadata', {})
                        })
                        
                        if len(results) >= max_results:
                            break
                            
            except Exception as e:
                logger.error(f"Error reading document {doc_path}: {e}")
                continue
        
        # Sort by relevance (number of occurrences)
        results.sort(key=lambda x: x['occurrences'], reverse=True)
        
        return results[:max_results]
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the search index."""
        total_size = sum(
            (self.index_dir / meta['filename']).stat().st_size
            for meta in self.metadata.values()
            if (self.index_dir / meta['filename']).exists()
        )
        
        return {
            'total_documents': len(self.metadata),
            'index_size_bytes': total_size,
            'index_directory': str(self.index_dir)
        }


# Example usage and testing
if __name__ == "__main__":
    # Set up logging using centralized configuration
    import logging_config
    
    # Create search engine instance
    engine = SimpleSearchEngine()
    
    # Test feeding documents
    print("Testing document indexing...")
    engine.feed_document(
        doc_id="test1",
        title="Introduction to Python",
        content="Python is a high-level programming language known for its simplicity and readability.",
        metadata={"category": "programming", "author": "Test Author"}
    )
    
    engine.feed_document(
        doc_id="test2",
        title="Web Development with Python",
        content="Python is widely used in web development with frameworks like Django and Flask.",
        metadata={"category": "web", "author": "Test Author"}
    )
    
    # Test querying
    print("\nTesting search functionality...")
    results = engine.query("Python")
    print(f"Found {len(results)} results for 'Python':")
    for result in results:
        print(f"- {result['title']} (occurrences: {result['occurrences']})")
        print(f"  Snippet: {result['snippet'][:100]}...")
    
    # Show stats
    print("\nIndex statistics:")
    stats = engine.get_stats()
    for key, value in stats.items():
        print(f"- {key}: {value}")