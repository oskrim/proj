#!/usr/bin/env python3
"""
Centralized logging configuration for the wiki project.

This module provides a consistent logging setup across all modules.
Import this at the beginning of your main script or entry points.
"""

import logging
import sys


def setup_logging(level=logging.INFO, format_string=None):
    """
    Configure logging for the entire application.
    
    Args:
        level: The logging level (default: logging.INFO)
        format_string: Custom format string (optional)
    """
    if format_string is None:
        format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Configure root logger
    logging.basicConfig(
        level=level,
        format=format_string,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Set consistent level for all project loggers
    for logger_name in ['graph_database', 'graph_extractor', 'wikipedia_indexer', 'simple_search_engine', 'test_graph_extractor']:
        logger = logging.getLogger(logger_name)
        logger.setLevel(level)


# Call this when the module is imported to set up default configuration
setup_logging()