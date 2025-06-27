#!/usr/bin/env python3
"""
Database operations for storing and retrieving graph data from PostgreSQL.
"""

import os
import logging
import logging_config  # Centralized logging configuration
from typing import List, Tuple, Optional, Dict, Any
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from contextlib import contextmanager

from graph_extractor import ExtractionResult

logger = logging.getLogger(__name__)


class GraphDatabase:
    """Handles PostgreSQL operations for graph data storage and retrieval."""

    def __init__(self,
                 host: str = None,
                 port: int = None,
                 database: str = None,
                 user: str = None,
                 password: str = None):
        """
        Initialize database connection parameters.

        Args:
            host: Database host (defaults to env var POSTGRES_HOST or 'localhost')
            port: Database port (defaults to env var POSTGRES_PORT or 5432)
            database: Database name (defaults to env var POSTGRES_DB or 'wiki')
            user: Database user (defaults to env var POSTGRES_USER or 'postgres')
            password: Database password (defaults to env var POSTGRES_PASSWORD)
        """
        self.host = host or os.getenv('POSTGRES_HOST', 'localhost')
        self.port = port or int(os.getenv('POSTGRES_PORT', '5432'))
        self.database = database or os.getenv('POSTGRES_DB', 'wiki_test')
        self.user = user or os.getenv('POSTGRES_USER', 'postgres')
        self.password = password or os.getenv('POSTGRES_PASSWORD', 'postgres')

    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                cursor_factory=RealDictCursor
            )
            yield conn
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            if conn:
                conn.commit()
                conn.close()

    def store_extraction_result(self,
                               doc_id: str,
                               title: str,
                               extraction_result: ExtractionResult,
                               metadata: Dict[str, Any] = None) -> Dict[str, int]:
        """
        Store extracted entities and relations in the database.

        Args:
            doc_id: Document identifier
            title: Document title
            extraction_result: Result from graph extraction
            metadata: Additional metadata

        Returns:
            Dictionary with counts of stored entities and relations
        """
        if metadata is None:
            metadata = {}

        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Store entities and collect their IDs
                entity_id_map = {}
                entities_stored = 0

                for entity_text, entity_type in extraction_result.entities:
                    # Check if entity already exists (by normalized name)
                    normalized_name = entity_text.lower().strip()

                    cur.execute("""
                        SELECT id FROM entities
                        WHERE normalized_name = %s AND entity_type = %s
                        LIMIT 1
                    """, (normalized_name, entity_type))

                    existing_entity = cur.fetchone()

                    if existing_entity:
                        entity_id_map[entity_text] = existing_entity['id']
                        logger.warning(f"Entity {entity_text} already exists")
                    else:
                        # Insert new entity
                        cur.execute("""
                            INSERT INTO entities (name, entity_type, normalized_name, document_id, metadata)
                            VALUES (%s, %s, %s, %s, %s)
                            RETURNING id
                        """, (entity_text, entity_type, normalized_name, doc_id, Json(metadata)))

                        entity_id = cur.fetchone()['id']
                        entity_id_map[entity_text] = entity_id
                        entities_stored += 1

                # Store relations
                relations_stored = 0

                for head_text, relation_type, tail_text in extraction_result.relations:
                    # Skip relations where entities weren't extracted
                    if head_text not in entity_id_map or tail_text not in entity_id_map:
                        logger.warning(f"Skipping relation {head_text} -> {relation_type} -> {tail_text}: entities not found")
                        continue

                    head_entity_id = entity_id_map[head_text]
                    tail_entity_id = entity_id_map[tail_text]

                    # Check if relation already exists
                    cur.execute("""
                        SELECT id FROM relationships
                        WHERE head_entity_id = %s AND tail_entity_id = %s AND relation_type = %s
                        LIMIT 1
                    """, (head_entity_id, tail_entity_id, relation_type))

                    if cur.fetchone():
                        logger.warning(f"Relation {head_text} -> {relation_type} -> {tail_text} already exists")
                    else:
                        # Insert new relation
                        source_text = f"{head_text} {relation_type} {tail_text}"
                        cur.execute("""
                            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type, source_text, metadata)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (head_entity_id, tail_entity_id, relation_type, source_text, Json(metadata)))
                        relations_stored += 1

                conn.commit()

                logger.info(f"Stored {entities_stored} new entities and {relations_stored} new relations for document {doc_id}")

                return {
                    'entities_stored': entities_stored,
                    'relations_stored': relations_stored,
                    'total_entities': len(extraction_result.entities),
                    'total_relations': len(extraction_result.relations)
                }

    def get_document_graph(self, doc_id: str) -> Dict[str, Any]:
        """
        Retrieve the graph data for a specific document.

        Args:
            doc_id: Document identifier

        Returns:
            Dictionary containing entities and relations for the document
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Get entities for the document
                cur.execute("""
                    SELECT id, name, entity_type, normalized_name, confidence, metadata
                    FROM entities
                    WHERE document_id = %s
                    ORDER BY name
                """, (doc_id,))

                entities = cur.fetchall()

                # Get relations for the document entities
                cur.execute("""
                    SELECT r.id, r.relation_type, r.confidence, r.source_text,
                           he.name as head_entity_name, he.entity_type as head_entity_type,
                           te.name as tail_entity_name, te.entity_type as tail_entity_type
                    FROM relationships r
                    JOIN entities he ON r.head_entity_id = he.id
                    JOIN entities te ON r.tail_entity_id = te.id
                    WHERE he.document_id = %s OR te.document_id = %s
                    ORDER BY r.confidence DESC
                """, (doc_id, doc_id))

                relations = cur.fetchall()

                return {
                    'document_id': doc_id,
                    'entities': [dict(entity) for entity in entities],
                    'relations': [dict(relation) for relation in relations]
                }

    def search_entities(self, search_term: str, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Search for entities by name.

        Args:
            search_term: Term to search for
            limit: Maximum number of results

        Returns:
            List of matching entities with relationship counts
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM find_entities_by_name(%s, 0.3, %s)
                """, (search_term, limit))

                return [dict(row) for row in cur.fetchall()]

    def get_entity_neighbors(self, entity_name: str, max_depth: int = 1) -> List[Dict[str, Any]]:
        """
        Get neighbors of an entity.

        Args:
            entity_name: Name of the entity
            max_depth: Maximum traversal depth

        Returns:
            List of neighboring entities with relationship information
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # First find the entity ID
                cur.execute("""
                    SELECT id FROM entities WHERE name = %s LIMIT 1
                """, (entity_name,))

                entity = cur.fetchone()
                if not entity:
                    return []

                # Get neighbors using the SQL function
                cur.execute("""
                    SELECT * FROM get_entity_neighbors(%s, %s)
                """, (entity['id'], max_depth))

                return [dict(row) for row in cur.fetchall()]

    def test_connection(self) -> bool:
        """Test database connection and basic functionality."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                result = cur.fetchone()
                return result is not None

    def get_all_entities(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get all entities from the database.

        Args:
            limit: Maximum number of entities to return (None for all)
            offset: Number of entities to skip

        Returns:
            List of entity dictionaries
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT id, name, entity_type, normalized_name,
                           confidence, document_id, metadata, created_at
                    FROM entities
                    ORDER BY created_at DESC, name
                """
                if limit:
                    query += f" LIMIT {limit} OFFSET {offset}"

                cur.execute(query)
                return [dict(row) for row in cur.fetchall()]

    def get_all_relations(self, limit: Optional[int] = None, offset: int = 0) -> List[Dict[str, Any]]:
        """
        Get all relations from the database with entity names.

        Args:
            limit: Maximum number of relations to return (None for all)
            offset: Number of relations to skip

        Returns:
            List of relation dictionaries with entity names
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                query = """
                    SELECT r.id, r.relation_type, r.confidence, r.source_text,
                           r.metadata, r.created_at,
                           he.name as head_entity_name, he.entity_type as head_entity_type,
                           te.name as tail_entity_name, te.entity_type as tail_entity_type
                    FROM relationships r
                    JOIN entities he ON r.head_entity_id = he.id
                    JOIN entities te ON r.tail_entity_id = te.id
                    ORDER BY r.created_at DESC
                """
                if limit:
                    query += f" LIMIT {limit} OFFSET {offset}"

                cur.execute(query)
                return [dict(row) for row in cur.fetchall()]

    def get_full_graph(self) -> Dict[str, Any]:
        """
        Get the complete graph with all entities and relations.

        Returns:
            Dictionary containing all entities and relations
        """
        entities = self.get_all_entities()
        relations = self.get_all_relations()

        return {
            'entities': entities,
            'relations': relations,
            'entity_count': len(entities),
            'relation_count': len(relations)
        }

    def get_entity_count(self) -> int:
        """Get total number of entities in the database."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as count FROM entities")
                return cur.fetchone()['count']

    def get_relation_count(self) -> int:
        """Get total number of relations in the database."""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) as count FROM relationships")
                return cur.fetchone()['count']

    def get_entities_by_type(self, entity_type: str) -> List[Dict[str, Any]]:
        """
        Get all entities of a specific type.

        Args:
            entity_type: The entity type to filter by (e.g., 'PERSON', 'ORGANIZATION')

        Returns:
            List of entities of the specified type
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, name, entity_type, normalized_name,
                           confidence, document_id, metadata, created_at
                    FROM entities
                    WHERE entity_type = %s
                    ORDER BY name
                """, (entity_type,))
                return [dict(row) for row in cur.fetchall()]

    def get_relations_by_type(self, relation_type: str) -> List[Dict[str, Any]]:
        """
        Get all relations of a specific type.

        Args:
            relation_type: The relation type to filter by (e.g., 'born_in', 'worked_for')

        Returns:
            List of relations of the specified type
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT r.id, r.relation_type, r.confidence, r.source_text,
                           r.metadata, r.created_at,
                           he.name as head_entity_name, he.entity_type as head_entity_type,
                           te.name as tail_entity_name, te.entity_type as tail_entity_type
                    FROM relationships r
                    JOIN entities he ON r.head_entity_id = he.id
                    JOIN entities te ON r.tail_entity_id = te.id
                    WHERE r.relation_type = %s
                    ORDER BY r.confidence DESC
                """, (relation_type,))
                return [dict(row) for row in cur.fetchall()]

    def get_graph_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about the graph.

        Returns:
            Dictionary with various graph statistics
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                # Entity statistics
                cur.execute("""
                    SELECT entity_type, COUNT(*) as count
                    FROM entities
                    GROUP BY entity_type
                    ORDER BY count DESC
                """)
                entity_type_counts = {row['entity_type']: row['count'] for row in cur.fetchall()}

                # Relation statistics
                cur.execute("""
                    SELECT relation_type, COUNT(*) as count
                    FROM relationships
                    GROUP BY relation_type
                    ORDER BY count DESC
                """)
                relation_type_counts = {row['relation_type']: row['count'] for row in cur.fetchall()}

                # Document statistics
                cur.execute("""
                    SELECT COUNT(DISTINCT document_id) as doc_count
                    FROM entities
                """)
                doc_count = cur.fetchone()['doc_count']

                # Most connected entities
                cur.execute("""
                    SELECT e.name, e.entity_type,
                           COUNT(DISTINCT r.id) as connection_count
                    FROM entities e
                    LEFT JOIN relationships r ON e.id = r.head_entity_id OR e.id = r.tail_entity_id
                    GROUP BY e.id, e.name, e.entity_type
                    ORDER BY connection_count DESC
                    LIMIT 10
                """)
                most_connected = [dict(row) for row in cur.fetchall()]

                return {
                    'total_entities': self.get_entity_count(),
                    'total_relations': self.get_relation_count(),
                    'total_documents': doc_count,
                    'entity_types': entity_type_counts,
                    'relation_types': relation_type_counts,
                    'most_connected_entities': most_connected
                }
