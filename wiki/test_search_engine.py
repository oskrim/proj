#!/usr/bin/env python3
"""
Unit tests for SimpleSearchEngine and wikipedia indexer functionality.
"""

import unittest
import tempfile
import shutil
import json
from pathlib import Path
from unittest.mock import patch, MagicMock
from simple_search_engine import SimpleSearchEngine
from wikipedia_indexer import index_document, search_engine


class TestSimpleSearchEngine(unittest.TestCase):
    """Test cases for the SimpleSearchEngine class."""
    
    def setUp(self):
        """Set up test environment before each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.engine = SimpleSearchEngine(index_dir=self.temp_dir)
    
    def tearDown(self):
        """Clean up test environment after each test."""
        shutil.rmtree(self.temp_dir)
    
    def test_initialization(self):
        """Test search engine initialization."""
        self.assertTrue(Path(self.temp_dir).exists())
        self.assertEqual(self.engine.index_dir, Path(self.temp_dir))
        self.assertTrue(self.engine.metadata_file.exists())
        self.assertEqual(self.engine.metadata, {})
    
    def test_sanitize_filename(self):
        """Test filename sanitization."""
        test_cases = [
            ("normal_id", "normal_id"),
            ("id/with/slashes", "id_with_slashes"),
            ("id:with:colons", "id_with_colons"),
            ("id<with>brackets", "id_with_brackets"),
            ("a" * 300, "a" * 200),  # Test length limiting
        ]
        
        for input_id, expected in test_cases:
            result = self.engine._sanitize_filename(input_id)
            self.assertEqual(result, expected)
    
    def test_feed_document(self):
        """Test document feeding functionality."""
        doc_id = "test_doc_1"
        title = "Test Document"
        content = "This is a test document with some content."
        metadata = {"author": "Test Author", "category": "test"}
        
        self.engine.feed_document(doc_id, title, content, metadata)
        
        # Check metadata was saved
        self.assertIn(doc_id, self.engine.metadata)
        doc_meta = self.engine.metadata[doc_id]
        self.assertEqual(doc_meta['title'], title)
        self.assertEqual(doc_meta['metadata'], metadata)
        self.assertEqual(doc_meta['content_length'], len(content))
        
        # Check document file was created
        doc_path = self.engine.index_dir / doc_meta['filename']
        self.assertTrue(doc_path.exists())
        
        # Check file content
        with open(doc_path, 'r', encoding='utf-8') as f:
            file_content = f.read()
            self.assertIn(title, file_content)
            self.assertIn(content, file_content)
    
    def test_feed_document_empty_id(self):
        """Test feeding document with empty ID raises error."""
        with self.assertRaises(ValueError):
            self.engine.feed_document("", "Title", "Content")
    
    def test_query_basic(self):
        """Test basic query functionality."""
        # Add test documents
        self.engine.feed_document("doc1", "Python Programming", 
                                  "Python is a great programming language.")
        self.engine.feed_document("doc2", "Java Programming", 
                                  "Java is also a programming language.")
        self.engine.feed_document("doc3", "Python Web Development", 
                                  "Python is used for web development.")
        
        # Query for "Python"
        results = self.engine.query("Python")
        self.assertEqual(len(results), 2)
        
        # Check results contain Python documents
        titles = [r['title'] for r in results]
        self.assertIn("Python Programming", titles)
        self.assertIn("Python Web Development", titles)
        self.assertNotIn("Java Programming", titles)
    
    def test_query_empty_string(self):
        """Test querying with empty string returns empty results."""
        self.engine.feed_document("doc1", "Test", "Test content")
        results = self.engine.query("")
        self.assertEqual(results, [])
    
    def test_query_case_insensitive(self):
        """Test case-insensitive search."""
        self.engine.feed_document("doc1", "Test Document", 
                                  "This contains PYTHON in uppercase.")
        
        results = self.engine.query("python")
        self.assertEqual(len(results), 1)
        self.assertIn("PYTHON", results[0]['snippet'])
    
    def test_query_relevance_sorting(self):
        """Test results are sorted by relevance (occurrence count)."""
        self.engine.feed_document("doc1", "Single Mention", 
                                  "Python is mentioned once here.")
        self.engine.feed_document("doc2", "Multiple Mentions", 
                                  "Python Python Python - mentioned three times!")
        
        results = self.engine.query("Python")
        self.assertEqual(len(results), 2)
        # Document with more occurrences should be first
        self.assertEqual(results[0]['title'], "Multiple Mentions")
        self.assertEqual(results[0]['occurrences'], 3)
        self.assertEqual(results[1]['occurrences'], 1)
    
    def test_query_max_results(self):
        """Test max_results parameter."""
        # Add 5 documents
        for i in range(5):
            self.engine.feed_document(f"doc{i}", f"Document {i}", 
                                      "This is a test document.")
        
        results = self.engine.query("test", max_results=3)
        self.assertEqual(len(results), 3)
    
    def test_get_stats(self):
        """Test statistics retrieval."""
        # Add some documents
        self.engine.feed_document("doc1", "Test 1", "Content 1")
        self.engine.feed_document("doc2", "Test 2", "Content 2")
        
        stats = self.engine.get_stats()
        self.assertEqual(stats['total_documents'], 2)
        self.assertEqual(stats['index_directory'], str(self.temp_dir))
        self.assertIn('index_size_bytes', stats)
        self.assertGreater(stats['index_size_bytes'], 0)


class TestWikipediaIndexer(unittest.TestCase):
    """Test cases for wikipedia indexer functions."""
    
    def setUp(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp()
        self.mock_engine = MagicMock()
        
    def tearDown(self):
        """Clean up test environment."""
        shutil.rmtree(self.temp_dir)
    
    @patch('wikipedia_indexer.search_engine')
    def test_index_document(self, mock_search_engine):
        """Test index_document function."""
        doc_id = "test123"
        title = "Test Article"
        content = "This is test content."
        metadata = {"url": "http://example.com", "language": "en"}
        
        index_document(doc_id, title, content, metadata)
        
        # Verify feed_document was called with correct parameters
        mock_search_engine.feed_document.assert_called_once_with(
            doc_id, title, content, metadata
        )
    
    @patch('wikipedia_indexer.load_dataset')
    @patch('wikipedia_indexer.search_engine')
    def test_download_and_index_wikipedia(self, mock_search_engine, mock_load_dataset):
        """Test download_and_index_wikipedia function."""
        from wikipedia_indexer import download_and_index_wikipedia
        
        # Mock dataset articles
        mock_articles = [
            {
                'id': '1',
                'title': 'Article 1',
                'text': 'Content 1',
                'url': 'http://wiki.com/1'
            },
            {
                'id': '2',
                'title': 'Article 2',
                'text': 'Content 2',
                'url': 'http://wiki.com/2'
            },
            {
                'id': '3',
                'title': '',  # Empty title, should be skipped
                'text': 'Content 3',
                'url': 'http://wiki.com/3'
            }
        ]
        
        mock_load_dataset.return_value = iter(mock_articles)
        
        # Test indexing 2 documents
        download_and_index_wikipedia(n_documents=2)
        
        # Should have called feed_document twice (skipping the empty title)
        self.assertEqual(mock_search_engine.feed_document.call_count, 2)
        
        # Check the calls were made with correct data
        calls = mock_search_engine.feed_document.call_args_list
        
        # First call
        self.assertEqual(calls[0][0][0], '1')  # doc_id
        self.assertEqual(calls[0][0][1], 'Article 1')  # title
        self.assertEqual(calls[0][0][2], 'Content 1')  # content
        
        # Second call
        self.assertEqual(calls[1][0][0], '2')  # doc_id
        self.assertEqual(calls[1][0][1], 'Article 2')  # title
        self.assertEqual(calls[1][0][2], 'Content 2')  # content


if __name__ == '__main__':
    unittest.main()