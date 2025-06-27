-- Migration 001: Create Graph Tables for GraphRAG
-- This migration extends the existing search engine with graph storage capabilities

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Entities table: stores extracted entities from documents
CREATE TABLE entities (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    name VARCHAR(500) NOT NULL,
    entity_type VARCHAR(100) NOT NULL,
    normalized_name VARCHAR(500), -- For deduplication and matching
    document_id INTEGER, -- Optional reference to source document
    confidence FLOAT DEFAULT 0.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    source_chunk_id INTEGER, -- Reference to specific chunk where entity was found
    metadata JSONB DEFAULT '{}', -- Additional entity metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Relationships table: stores extracted relationships between entities
CREATE TABLE relationships (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    head_entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    tail_entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    relation_type VARCHAR(200) NOT NULL,
    confidence FLOAT DEFAULT 0.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    source_chunk_id INTEGER, -- Reference to chunk where relationship was found
    source_text TEXT, -- Original text snippet containing the relationship
    metadata JSONB DEFAULT '{}', -- Additional relationship metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    -- Prevent self-relationships and duplicate relationships
    CONSTRAINT no_self_relationships CHECK (head_entity_id != tail_entity_id),
    CONSTRAINT unique_relationships UNIQUE (head_entity_id, tail_entity_id, relation_type)
);

-- Communities table: stores detected communities from graph analysis
CREATE TABLE communities (
    id SERIAL PRIMARY KEY,
    uuid UUID DEFAULT uuid_generate_v4() UNIQUE NOT NULL,
    name VARCHAR(500), -- Optional human-readable name
    summary TEXT, -- LLM-generated community summary
    summary_embedding VECTOR(384), -- Embedding of the community summary
    size INTEGER DEFAULT 0, -- Number of entities in the community
    density FLOAT DEFAULT 0.0, -- Graph density metric for the community
    document_id INTEGER, -- Optional reference if community is document-specific
    algorithm VARCHAR(100) DEFAULT 'leiden', -- Algorithm used for detection
    metadata JSONB DEFAULT '{}', -- Additional community metadata
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Entity-Community mapping: many-to-many relationship
CREATE TABLE entity_communities (
    entity_id INTEGER NOT NULL REFERENCES entities(id) ON DELETE CASCADE,
    community_id INTEGER NOT NULL REFERENCES communities(id) ON DELETE CASCADE,
    membership_strength FLOAT DEFAULT 1.0, -- Strength of entity's membership in community
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (entity_id, community_id)
);

-- Graph statistics table: for caching expensive computations
CREATE TABLE graph_statistics (
    id SERIAL PRIMARY KEY,
    document_id INTEGER, -- NULL for global statistics
    stat_name VARCHAR(100) NOT NULL,
    stat_value JSONB NOT NULL,
    computed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    UNIQUE (document_id, stat_name)
);

-- Indexes for performance

-- Entity indexes
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_entities_normalized_name ON entities(normalized_name);
CREATE INDEX idx_entities_type ON entities(entity_type);
CREATE INDEX idx_entities_document_id ON entities(document_id);
CREATE INDEX idx_entities_source_chunk_id ON entities(source_chunk_id);
CREATE INDEX idx_entities_confidence ON entities(confidence);
CREATE INDEX idx_entities_created_at ON entities(created_at);

-- GIN index for entity metadata JSONB queries
CREATE INDEX idx_entities_metadata ON entities USING GIN(metadata);

-- Relationship indexes
CREATE INDEX idx_relationships_head_entity ON relationships(head_entity_id);
CREATE INDEX idx_relationships_tail_entity ON relationships(tail_entity_id);
CREATE INDEX idx_relationships_relation_type ON relationships(relation_type);
CREATE INDEX idx_relationships_confidence ON relationships(confidence);
CREATE INDEX idx_relationships_source_chunk_id ON relationships(source_chunk_id);
CREATE INDEX idx_relationships_created_at ON relationships(created_at);

-- Composite index for efficient graph traversal
CREATE INDEX idx_relationships_traversal ON relationships(head_entity_id, tail_entity_id, relation_type);

-- GIN index for relationship metadata JSONB queries
CREATE INDEX idx_relationships_metadata ON relationships USING GIN(metadata);

-- Community indexes
CREATE INDEX idx_communities_document_id ON communities(document_id);
CREATE INDEX idx_communities_size ON communities(size);
CREATE INDEX idx_communities_algorithm ON communities(algorithm);
CREATE INDEX idx_communities_created_at ON communities(created_at);

-- HNSW index for community summary embeddings (requires pgvector)
-- Note: Adjust dimensions if using different embedding model
CREATE INDEX idx_communities_summary_embedding ON communities 
USING hnsw (summary_embedding vector_cosine_ops)
WITH (m = 16, ef_construction = 64);

-- GIN index for community metadata
CREATE INDEX idx_communities_metadata ON communities USING GIN(metadata);

-- Entity-Community indexes
CREATE INDEX idx_entity_communities_entity_id ON entity_communities(entity_id);
CREATE INDEX idx_entity_communities_community_id ON entity_communities(community_id);
CREATE INDEX idx_entity_communities_membership_strength ON entity_communities(membership_strength);

-- Graph statistics indexes
CREATE INDEX idx_graph_statistics_document_id ON graph_statistics(document_id);
CREATE INDEX idx_graph_statistics_stat_name ON graph_statistics(stat_name);
CREATE INDEX idx_graph_statistics_computed_at ON graph_statistics(computed_at);

-- Triggers for updated_at timestamps
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_entities_updated_at BEFORE UPDATE ON entities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_relationships_updated_at BEFORE UPDATE ON relationships
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_communities_updated_at BEFORE UPDATE ON communities
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Views for common queries

-- Entity relationship view with entity names
CREATE VIEW entity_relationships AS
SELECT 
    r.id,
    r.uuid,
    he.name AS head_entity_name,
    he.entity_type AS head_entity_type,
    r.relation_type,
    te.name AS tail_entity_name,
    te.entity_type AS tail_entity_type,
    r.confidence,
    r.source_text,
    r.created_at
FROM relationships r
JOIN entities he ON r.head_entity_id = he.id
JOIN entities te ON r.tail_entity_id = te.id;

-- Community entities view
CREATE VIEW community_entities AS
SELECT 
    c.id AS community_id,
    c.name AS community_name,
    c.summary AS community_summary,
    e.id AS entity_id,
    e.name AS entity_name,
    e.entity_type,
    ec.membership_strength
FROM communities c
JOIN entity_communities ec ON c.id = ec.community_id
JOIN entities e ON ec.entity_id = e.id;

-- Graph connectivity view (entity degrees)
CREATE VIEW entity_connectivity AS
SELECT 
    e.id,
    e.name,
    e.entity_type,
    COALESCE(out_degree.count, 0) + COALESCE(in_degree.count, 0) AS total_degree,
    COALESCE(out_degree.count, 0) AS out_degree,
    COALESCE(in_degree.count, 0) AS in_degree
FROM entities e
LEFT JOIN (
    SELECT head_entity_id, COUNT(*) as count
    FROM relationships
    GROUP BY head_entity_id
) out_degree ON e.id = out_degree.head_entity_id
LEFT JOIN (
    SELECT tail_entity_id, COUNT(*) as count
    FROM relationships
    GROUP BY tail_entity_id
) in_degree ON e.id = in_degree.tail_entity_id;