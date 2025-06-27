#!/usr/bin/env python3
"""
Unit tests for PostgreSQL graph schema and functions.
Tests the graph tables, indexes, and SQL functions for GraphRAG implementation.
"""

import unittest
import psycopg2
import psycopg2.extras
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import tempfile
import os
from unittest.mock import patch, MagicMock
import json


class TestGraphSchema(unittest.TestCase):
    """Test cases for the graph database schema and functions."""

    @classmethod
    def setUpClass(cls):
        """Set up test database connection."""
        # Use environment variables or defaults for test database
        cls.db_config = {
            'host': os.getenv('TEST_DB_HOST', 'localhost'),
            'port': os.getenv('TEST_DB_PORT', '5432'),
            'user': os.getenv('TEST_DB_USER', 'postgres'),
            'password': os.getenv('TEST_DB_PASSWORD', 'postgres'),
            'database': os.getenv('TEST_DB_NAME', 'wiki_test')
        }

        # Create test database if it doesn't exist
        try:
            conn = psycopg2.connect(
                host=cls.db_config['host'],
                port=cls.db_config['port'],
                user=cls.db_config['user'],
                password=cls.db_config['password'],
                database='postgres'
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

            with conn.cursor() as cur:
                cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{cls.db_config['database']}'")
                if not cur.fetchone():
                    cur.execute(f"CREATE DATABASE {cls.db_config['database']}")

            conn.close()
        except Exception as e:
            print(f"Warning: Could not create test database: {e}")

    def setUp(self):
        """Set up test environment before each test."""
        self.conn = psycopg2.connect(**self.db_config)
        self.conn.autocommit = True
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        # Apply migrations
        self._apply_migrations()

    def tearDown(self):
        """Clean up test environment after each test."""
        if hasattr(self, 'cursor') and self.cursor:
            # Clean up test data
            self.cursor.execute("TRUNCATE TABLE entity_communities, relationships, entities, communities, graph_statistics CASCADE")
            self.cursor.close()

        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

    def _apply_migrations(self):
        """Apply database migrations for testing."""
        migration_files = [
            '/home/oskari/git/wiki/migrations/001_create_graph_tables.sql',
            '/home/oskari/git/wiki/migrations/002_create_graph_functions.sql'
        ]

        for migration_file in migration_files:
            if os.path.exists(migration_file):
                with open(migration_file, 'r') as f:
                    migration_sql = f.read()
                try:
                    self.cursor.execute(migration_sql)
                except Exception as e:
                    print(f"Migration error in {migration_file}: {e}")

    def test_table_creation(self):
        """Test that all required tables are created."""
        expected_tables = [
            'entities', 'relationships', 'communities',
            'entity_communities', 'graph_statistics'
        ]

        self.cursor.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """)

        actual_tables = [row['table_name'] for row in self.cursor.fetchall()]

        for table in expected_tables:
            self.assertIn(table, actual_tables, f"Table {table} not found")

    def test_entity_insertion(self):
        """Test entity insertion and retrieval."""
        # Insert test entity
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type, confidence, metadata)
            VALUES (%s, %s, %s, %s)
            RETURNING id, uuid, name, entity_type, confidence
        """, ("Test Entity", "PERSON", 0.95, json.dumps({"test": "data"})))

        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result['name'], "Test Entity")
        self.assertEqual(result['entity_type'], "PERSON")
        self.assertEqual(result['confidence'], 0.95)

        # Test retrieval
        entity_id = result['id']
        self.cursor.execute("SELECT * FROM entities WHERE id = %s", (entity_id,))
        retrieved = self.cursor.fetchone()
        self.assertEqual(retrieved['name'], "Test Entity")

    def test_relationship_insertion(self):
        """Test relationship insertion with foreign key constraints."""
        # Create two entities first
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('Entity A', 'PERSON'), ('Entity B', 'ORGANIZATION')
            RETURNING id
        """)

        entity_ids = [row['id'] for row in self.cursor.fetchall()]
        self.assertEqual(len(entity_ids), 2)

        # Insert relationship
        self.cursor.execute("""
            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type, confidence, source_text)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, relation_type
        """, (entity_ids[0], entity_ids[1], "works_for", 0.85, "Entity A works for Entity B"))

        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result['relation_type'], "works_for")

    def test_relationship_constraints(self):
        """Test relationship constraints (no self-relationships, unique constraints)."""
        # Create entity
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type) VALUES ('Test Entity', 'PERSON')
            RETURNING id
        """)
        entity_id = self.cursor.fetchone()['id']

        # Test self-relationship prevention
        with self.assertRaises(psycopg2.IntegrityError):
            self.cursor.execute("""
                INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type)
                VALUES (%s, %s, 'self_relation')
            """, (entity_id, entity_id))

        # Reset connection after integrity error
        self.conn.rollback()

    def test_community_creation(self):
        """Test community creation and entity assignment."""
        # Create community
        self.cursor.execute("""
            INSERT INTO communities (name, summary, size, algorithm)
            VALUES (%s, %s, %s, %s)
            RETURNING id, name
        """, ("Test Community", "A test community for testing", 2, "leiden"))

        community = self.cursor.fetchone()
        self.assertIsNotNone(community)
        self.assertEqual(community['name'], "Test Community")

        # Create entities
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('Entity 1', 'PERSON'), ('Entity 2', 'PERSON')
            RETURNING id
        """)
        entity_ids = [row['id'] for row in self.cursor.fetchall()]

        # Assign entities to community
        for entity_id in entity_ids:
            self.cursor.execute("""
                INSERT INTO entity_communities (entity_id, community_id, membership_strength)
                VALUES (%s, %s, %s)
            """, (entity_id, community['id'], 1.0))

        # Verify assignments
        self.cursor.execute("""
            SELECT COUNT(*) as count FROM entity_communities
            WHERE community_id = %s
        """, (community['id'],))

        count = self.cursor.fetchone()['count']
        self.assertEqual(count, 2)

    def test_entity_relationships_view(self):
        """Test the entity_relationships view."""
        # Create test data
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('Alice', 'PERSON'), ('Acme Corp', 'ORGANIZATION')
            RETURNING id
        """)
        entity_ids = [row['id'] for row in self.cursor.fetchall()]

        self.cursor.execute("""
            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type, confidence)
            VALUES (%s, %s, %s, %s)
        """, (entity_ids[0], entity_ids[1], "employed_by", 0.9))

        # Query the view
        self.cursor.execute("""
            SELECT * FROM entity_relationships
            WHERE head_entity_name = 'Alice' AND tail_entity_name = 'Acme Corp'
        """)

        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result['head_entity_name'], 'Alice')
        self.assertEqual(result['tail_entity_name'], 'Acme Corp')
        self.assertEqual(result['relation_type'], 'employed_by')

    def test_get_entity_neighbors_function(self):
        """Test the get_entity_neighbors SQL function."""
        # Create test graph: A -> B -> C
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('A', 'PERSON'), ('B', 'PERSON'), ('C', 'PERSON')
            RETURNING id, name
        """)

        entities = {row['name']: row['id'] for row in self.cursor.fetchall()}

        # Create relationships
        self.cursor.execute("""
            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type, confidence)
            VALUES
                (%s, %s, 'knows', 0.9),
                (%s, %s, 'knows', 0.8)
        """, (entities['A'], entities['B'], entities['B'], entities['C']))

        # Test direct neighbors (depth 1)
        self.cursor.execute("""
            SELECT neighbor_name, depth FROM get_entity_neighbors(%s, 1)
            ORDER BY neighbor_name
        """, (entities['A'],))

        neighbors = self.cursor.fetchall()
        self.assertEqual(len(neighbors), 1)
        self.assertEqual(neighbors[0]['neighbor_name'], 'B')
        self.assertEqual(neighbors[0]['depth'], 1)

        # Test extended neighbors (depth 2)
        self.cursor.execute("""
            SELECT neighbor_name, depth FROM get_entity_neighbors(%s, 2)
            ORDER BY neighbor_name, depth
        """, (entities['A'],))

        neighbors = self.cursor.fetchall()
        neighbor_names = [n['neighbor_name'] for n in neighbors]
        self.assertIn('B', neighbor_names)
        self.assertIn('C', neighbor_names)

    def test_find_entity_path_function(self):
        """Test the find_entity_path SQL function."""
        # Create test graph: A -> B -> C
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('A', 'PERSON'), ('B', 'PERSON'), ('C', 'PERSON')
            RETURNING id, name
        """)

        entities = {row['name']: row['id'] for row in self.cursor.fetchall()}

        # Create path A -> B -> C
        self.cursor.execute("""
            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type, confidence)
            VALUES
                (%s, %s, 'knows', 0.9),
                (%s, %s, 'knows', 0.8)
        """, (entities['A'], entities['B'], entities['B'], entities['C']))

        # Find path from A to C
        self.cursor.execute("""
            SELECT path_length, entity_path FROM find_entity_path(%s, %s)
        """, (entities['A'], entities['C']))

        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result['path_length'], 2)
        self.assertEqual(len(result['entity_path']), 3)  # A, B, C
        self.assertEqual(result['entity_path'][0], entities['A'])
        self.assertEqual(result['entity_path'][-1], entities['C'])

    def test_find_entities_by_name_function(self):
        """Test the find_entities_by_name function with similarity search."""
        # Insert test entities with similar names
        test_entities = [
            ("John Smith", "PERSON"),
            ("John Doe", "PERSON"),
            ("Jane Smith", "PERSON"),
            ("Johnny Walker", "PERSON")
        ]

        for name, entity_type in test_entities:
            self.cursor.execute("""
                INSERT INTO entities (name, entity_type) VALUES (%s, %s)
            """, (name, entity_type))

        # Search for "John"
        self.cursor.execute("""
            SELECT entity_name, similarity_score
            FROM find_entities_by_name('John', 0.1, 10)
            ORDER BY similarity_score DESC
        """)

        results = self.cursor.fetchall()
        self.assertGreater(len(results), 0)

        # Results should include entities with "John" in the name
        names = [r['entity_name'] for r in results]
        self.assertIn("John Smith", names)
        self.assertIn("John Doe", names)

    def test_compute_graph_statistics_function(self):
        """Test the compute_graph_statistics function."""
        # Create test entities and relationships
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('A', 'PERSON'), ('B', 'PERSON'), ('C', 'PERSON')
            RETURNING id
        """)

        entity_ids = [row['id'] for row in self.cursor.fetchall()]

        # Create relationships
        self.cursor.execute("""
            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type)
            VALUES (%s, %s, 'knows'), (%s, %s, 'knows')
        """, (entity_ids[0], entity_ids[1], entity_ids[1], entity_ids[2]))

        # Compute statistics
        self.cursor.execute("SELECT compute_graph_statistics() as stats")
        result = self.cursor.fetchone()

        stats = result['stats']
        self.assertIsInstance(stats, dict)
        self.assertEqual(stats['entity_count'], 3)
        self.assertEqual(stats['relationship_count'], 2)
        self.assertIn('avg_degree', stats)
        self.assertIn('density', stats)

    def test_get_community_context_function(self):
        """Test the get_community_context function."""
        # Create test community and entities
        self.cursor.execute("""
            INSERT INTO communities (name, summary, size)
            VALUES ('Test Community', 'A test community', 2)
            RETURNING id
        """)
        community_id = self.cursor.fetchone()['id']

        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('Alice', 'PERSON'), ('Bob', 'PERSON')
            RETURNING id
        """)
        entity_ids = [row['id'] for row in self.cursor.fetchall()]

        # Assign entities to community
        for entity_id in entity_ids:
            self.cursor.execute("""
                INSERT INTO entity_communities (entity_id, community_id)
                VALUES (%s, %s)
            """, (entity_id, community_id))

        # Create relationship
        self.cursor.execute("""
            INSERT INTO relationships (head_entity_id, tail_entity_id, relation_type)
            VALUES (%s, %s, 'knows')
        """, (entity_ids[0], entity_ids[1]))

        # Get community context
        self.cursor.execute("""
            SELECT * FROM get_community_context(%s)
        """, (entity_ids,))

        result = self.cursor.fetchone()
        self.assertIsNotNone(result)
        self.assertEqual(result['community_name'], 'Test Community')
        self.assertEqual(result['entity_count'], 2)

    def test_indexes_exist(self):
        """Test that required indexes are created."""
        self.cursor.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename IN ('entities', 'relationships', 'communities')
        """)

        indexes = [row['indexname'] for row in self.cursor.fetchall()]

        # Check for some key indexes
        expected_indexes = [
            'idx_entities_name',
            'idx_entities_type',
            'idx_relationships_head_entity',
            'idx_relationships_tail_entity',
            'idx_communities_size'
        ]

        for index in expected_indexes:
            self.assertIn(index, indexes, f"Index {index} not found")

    def test_triggers_work(self):
        """Test that updated_at triggers work correctly."""
        # Insert entity
        self.cursor.execute("""
            INSERT INTO entities (name, entity_type)
            VALUES ('Test Entity', 'PERSON')
            RETURNING id, created_at, updated_at
        """)

        original = self.cursor.fetchone()
        entity_id = original['id']
        original_updated_at = original['updated_at']

        # Update entity
        import time
        time.sleep(0.1)  # Ensure timestamp difference

        self.cursor.execute("""
            UPDATE entities SET name = 'Updated Entity' WHERE id = %s
            RETURNING updated_at
        """, (entity_id,))

        updated = self.cursor.fetchone()
        self.assertGreater(updated['updated_at'], original_updated_at)


if __name__ == '__main__':
    # Set up test database connection info
    print("Running graph schema tests...")
    print("Make sure PostgreSQL is running and test database is accessible")
    print("Set TEST_DB_* environment variables if needed")

    unittest.main(verbosity=2)
