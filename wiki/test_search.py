#!/usr/bin/env python3
"""
Test script to query the indexed Wikipedia documents.
"""

import sys
from simple_search_engine import SimpleSearchEngine

def main():
    if len(sys.argv) < 2:
        print("Usage: python test_search.py <query>")
        print("Example: python test_search.py anarchism")
        sys.exit(1)
    
    query = " ".join(sys.argv[1:])
    
    # Initialize search engine with the same index directory
    engine = SimpleSearchEngine(index_dir="./wikipedia_index")
    
    # Get index stats
    stats = engine.get_stats()
    print(f"Search index contains {stats['total_documents']} documents")
    print(f"Total index size: {stats['index_size_bytes'] / 1024 / 1024:.2f} MB")
    print()
    
    # Perform search
    print(f"Searching for: '{query}'")
    print("-" * 50)
    
    results = engine.query(query, max_results=5)
    
    if not results:
        print("No results found.")
    else:
        print(f"Found {len(results)} results:\n")
        
        for i, result in enumerate(results, 1):
            print(f"{i}. {result['title']}")
            print(f"   Occurrences: {result['occurrences']}")
            print(f"   Snippet: {result['snippet']}")
            if result['metadata']:
                print(f"   URL: {result['metadata'].get('url', 'N/A')}")
            print()

if __name__ == "__main__":
    main()