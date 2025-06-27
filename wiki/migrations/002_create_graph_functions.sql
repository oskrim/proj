-- Migration 002: Create Graph Traversal Functions
-- SQL functions to replace NetworkX graph operations

-- Function to find direct neighbors of an entity
CREATE OR REPLACE FUNCTION get_entity_neighbors(
    input_entity_id INTEGER,
    max_depth INTEGER DEFAULT 1,
    relation_types TEXT[] DEFAULT NULL,
    min_confidence FLOAT DEFAULT 0.0
)
RETURNS TABLE (
    neighbor_id INTEGER,
    neighbor_name VARCHAR(500),
    neighbor_type VARCHAR(100),
    relation_type VARCHAR(200),
    confidence FLOAT,
    depth INTEGER,
    path INTEGER[]
) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE entity_graph AS (
        -- Non-recursive term (Base case): direct neighbors (outgoing and incoming)
        (
            SELECT
                r.tail_entity_id as neighbor_id,
                r.relation_type,
                r.confidence,
                1 as depth,
                ARRAY[input_entity_id, r.tail_entity_id] as path
            FROM relationships r
            WHERE r.head_entity_id = input_entity_id
                AND r.confidence >= min_confidence
                AND (relation_types IS NULL OR r.relation_type = ANY(relation_types))
            UNION -- Using UNION here to handle cases where an entity is a neighbor in both directions at depth 1
            SELECT
                r.head_entity_id as neighbor_id,
                r.relation_type,
                r.confidence,
                1 as depth,
                ARRAY[input_entity_id, r.head_entity_id] as path
            FROM relationships r
            WHERE r.tail_entity_id = input_entity_id
                AND r.confidence >= min_confidence
                AND (relation_types IS NULL OR r.relation_type = ANY(relation_types))
        )

        UNION ALL -- Separator

        -- Recursive term: a single SELECT statement exploring from the previous step
        SELECT
            next_step.target_entity,
            next_step.relation_type,
            next_step.confidence,
            eg.depth + 1,
            eg.path || next_step.target_entity
        FROM entity_graph eg
        JOIN (
            -- Subquery combining both outgoing and incoming relationships
            SELECT head_entity_id AS source_entity, tail_entity_id AS target_entity, r.relation_type, r.confidence FROM relationships r
            UNION ALL
            SELECT tail_entity_id AS source_entity, head_entity_id AS target_entity, r.relation_type, r.confidence FROM relationships r
        ) AS next_step ON eg.neighbor_id = next_step.source_entity
        WHERE eg.depth < max_depth
            AND next_step.confidence >= min_confidence
            AND (relation_types IS NULL OR next_step.relation_type = ANY(relation_types))
            AND NOT (next_step.target_entity = ANY(eg.path)) -- Prevent cycles
    )
    -- Final select joins to Entities table once at the end for performance
    SELECT DISTINCT
        eg.neighbor_id,
        e.name as neighbor_name,
        e.entity_type as neighbor_type,
        eg.relation_type,
        eg.confidence,
        eg.depth,
        eg.path
    FROM entity_graph eg
    JOIN entities e ON eg.neighbor_id = e.id
    ORDER BY eg.depth, eg.confidence DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to find shortest path between two entities
CREATE OR REPLACE FUNCTION find_entity_path(
    start_entity_id INTEGER,
    end_entity_id INTEGER,
    max_depth INTEGER DEFAULT 5,
    min_confidence FLOAT DEFAULT 0.0
)
RETURNS TABLE (
    path_length INTEGER,
    entity_path INTEGER[],
    relation_path VARCHAR[]
) AS $$
BEGIN
    RETURN QUERY
    WITH RECURSIVE path_search AS (
        -- Base case: start entity (Non-recursive term)
        SELECT
            start_entity_id as current_entity,
            0 as depth,
            ARRAY[start_entity_id] as path,
            ARRAY[]::VARCHAR[] as relations

        UNION ALL -- Separator between non-recursive and recursive terms

        -- Recursive term: a single SELECT statement
        SELECT
            next_step.target_entity,
            ps.depth + 1,
            ps.path || next_step.target_entity,
            ps.relations || next_step.relation_type
        FROM
            path_search ps
        JOIN (
            -- Subquery combining both outgoing and incoming relationships
            SELECT head_entity_id AS source_entity, tail_entity_id AS target_entity, relation_type, confidence FROM relationships
            UNION ALL
            SELECT tail_entity_id AS source_entity, head_entity_id AS target_entity, relation_type, confidence FROM relationships
        ) AS next_step ON ps.current_entity = next_step.source_entity
        WHERE
            ps.depth < max_depth
            AND next_step.confidence >= min_confidence
            AND NOT (next_step.target_entity = ANY(ps.path)) -- Prevent cycles
    )
    SELECT
        ps.depth as path_length,
        ps.path as entity_path,
        ps.relations as relation_path
    FROM path_search ps
    WHERE ps.current_entity = end_entity_id
    ORDER BY ps.depth
    LIMIT 1; -- Return shortest path only
END;
$$ LANGUAGE plpgsql;

-- Function to get entity subgraph (entity + neighbors + their relationships)
CREATE OR REPLACE FUNCTION get_entity_subgraph(
    entity_ids INTEGER[],
    depth INTEGER DEFAULT 1,
    min_confidence FLOAT DEFAULT 0.0
)
RETURNS TABLE (
    head_entity_id INTEGER,
    head_entity_name VARCHAR(500),
    head_entity_type VARCHAR(100),
    tail_entity_id INTEGER,
    tail_entity_name VARCHAR(500),
    tail_entity_type VARCHAR(100),
    relation_type VARCHAR(200),
    confidence FLOAT,
    source_text TEXT
) AS $$
BEGIN
    RETURN QUERY
    WITH expanded_entities AS (
        -- Get all entities within the specified depth
        SELECT DISTINCT neighbor_id as entity_id
        FROM unnest(entity_ids) as input_entity(id)
        CROSS JOIN LATERAL get_entity_neighbors(input_entity.id, depth, NULL, min_confidence)

        UNION

        -- Include the original entities
        SELECT unnest(entity_ids) as entity_id
    )
    SELECT
        r.head_entity_id,
        he.name as head_entity_name,
        he.entity_type as head_entity_type,
        r.tail_entity_id,
        te.name as tail_entity_name,
        te.entity_type as tail_entity_type,
        r.relation_type,
        r.confidence,
        r.source_text
    FROM relationships r
    JOIN entities he ON r.head_entity_id = he.id
    JOIN entities te ON r.tail_entity_id = te.id
    WHERE (r.head_entity_id IN (SELECT entity_id FROM expanded_entities)
           OR r.tail_entity_id IN (SELECT entity_id FROM expanded_entities))
        AND r.confidence >= min_confidence
    ORDER BY r.confidence DESC;
END;
$$ LANGUAGE plpgsql;

-- Function to find entities by name with fuzzy matching
CREATE OR REPLACE FUNCTION find_entities_by_name(
    search_name TEXT,
    similarity_threshold FLOAT DEFAULT 0.3,
    max_results INTEGER DEFAULT 10
)
RETURNS TABLE (
    entity_id INTEGER,
    entity_name VARCHAR(500),
    entity_type VARCHAR(100),
    similarity_score FLOAT,
    total_relationships INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        e.id,
        e.name,
        e.entity_type,
        similarity(e.name, search_name)::FLOAT as similarity_score,
        COALESCE(rel_count.count, 0)::INTEGER as total_relationships
    FROM entities e
    LEFT JOIN (
        SELECT
            all_rels.entity_id,
            COUNT(*) as count
        FROM (
            SELECT head_entity_id as entity_id FROM relationships
            UNION ALL
            SELECT tail_entity_id as entity_id FROM relationships
        ) all_rels
        GROUP BY all_rels.entity_id
    ) rel_count ON e.id = rel_count.entity_id
    WHERE similarity(e.name, search_name) >= similarity_threshold
        OR e.name ILIKE '%' || search_name || '%'
        OR e.normalized_name ILIKE '%' || search_name || '%'
    ORDER BY similarity_score DESC, total_relationships DESC
    LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

-- Function to compute graph statistics for a document or globally
CREATE OR REPLACE FUNCTION compute_graph_statistics(
    target_document_id INTEGER DEFAULT NULL
)
RETURNS JSONB AS $$
DECLARE
    stats JSONB := '{}';
    entity_count INTEGER;
    relationship_count INTEGER;
    avg_degree FLOAT;
    max_degree INTEGER;
    component_count INTEGER;
BEGIN
    -- Count entities
    IF target_document_id IS NULL THEN
        SELECT COUNT(*) INTO entity_count FROM entities;
        SELECT COUNT(*) INTO relationship_count FROM relationships;
    ELSE
        SELECT COUNT(*) INTO entity_count FROM entities WHERE document_id = target_document_id;
        SELECT COUNT(*) INTO relationship_count FROM relationships r
        JOIN entities he ON r.head_entity_id = he.id
        WHERE he.document_id = target_document_id;
    END IF;

    -- Compute average degree
    WITH degree_stats AS (
        SELECT
            AVG(total_degree) as avg_deg,
            MAX(total_degree) as max_deg
        FROM entity_connectivity e
        WHERE (target_document_id IS NULL OR e.id IN (
            SELECT id FROM entities WHERE document_id = target_document_id
        ))
    )
    SELECT avg_deg, max_deg INTO avg_degree, max_degree FROM degree_stats;

    -- Estimate connected components (simplified)
    -- Note: This is a simplified estimation, not exact connected components
    SELECT COUNT(DISTINCT e.id) INTO component_count
    FROM entities e
    LEFT JOIN relationships r ON (e.id = r.head_entity_id OR e.id = r.tail_entity_id)
    WHERE (target_document_id IS NULL OR e.document_id = target_document_id)
        AND r.id IS NULL; -- Isolated entities

    -- Build statistics JSON
    stats := jsonb_build_object(
        'entity_count', entity_count,
        'relationship_count', relationship_count,
        'avg_degree', COALESCE(avg_degree, 0),
        'max_degree', COALESCE(max_degree, 0),
        'isolated_entities', component_count,
        'density', CASE
            WHEN entity_count > 1 THEN
                (relationship_count::FLOAT / ((entity_count * (entity_count - 1)) / 2))
            ELSE 0
        END,
        'computed_at', NOW()
    );

    -- Cache the statistics
    INSERT INTO graph_statistics (document_id, stat_name, stat_value, computed_at)
    VALUES (target_document_id, 'basic_stats', stats, NOW())
    ON CONFLICT (document_id, stat_name)
    DO UPDATE SET stat_value = EXCLUDED.stat_value, computed_at = EXCLUDED.computed_at;

    RETURN stats;
END;
$$ LANGUAGE plpgsql;

-- Function to get community context for query processing
CREATE OR REPLACE FUNCTION get_community_context(
    entity_ids INTEGER[],
    include_neighbors BOOLEAN DEFAULT true
)
RETURNS TABLE (
    community_id INTEGER,
    community_name VARCHAR(500),
    community_summary TEXT,
    entity_count INTEGER,
    relevant_entities JSONB,
    related_relationships JSONB
) AS $$
BEGIN
    RETURN QUERY
    WITH relevant_communities AS (
        SELECT DISTINCT ec.community_id
        FROM entity_communities ec
        WHERE ec.entity_id = ANY(entity_ids)
    ),
    community_entities AS (
        SELECT
            rc.community_id,
            array_agg(e.id) as entity_list,
            array_agg(e.name) as entity_names,
            array_agg(e.entity_type) as entity_types
        FROM relevant_communities rc
        JOIN entity_communities ec ON rc.community_id = ec.community_id
        JOIN entities e ON ec.entity_id = e.id
        GROUP BY rc.community_id
    ),
    community_relationships AS (
        SELECT
            ce.community_id,
            jsonb_agg(jsonb_build_object(
                'head_entity', he.name,
                'relation', r.relation_type,
                'tail_entity', te.name,
                'confidence', r.confidence
            )) as relationships
        FROM community_entities ce
        JOIN relationships r ON (
            r.head_entity_id = ANY(ce.entity_list)
            AND r.tail_entity_id = ANY(ce.entity_list)
        )
        JOIN entities he ON r.head_entity_id = he.id
        JOIN entities te ON r.tail_entity_id = te.id
        GROUP BY ce.community_id
    )
    SELECT
        c.id,
        c.name,
        c.summary,
        c.size,
        jsonb_build_object(
            'entity_ids', ce.entity_list,
            'entity_names', ce.entity_names,
            'entity_types', ce.entity_types
        ) as relevant_entities,
        COALESCE(cr.relationships, '[]'::jsonb) as related_relationships
    FROM communities c
    JOIN community_entities ce ON c.id = ce.community_id
    LEFT JOIN community_relationships cr ON c.id = cr.community_id
    ORDER BY c.size DESC;
END;
$$ LANGUAGE plpgsql;

-- Enable pg_trgm extension for similarity search
CREATE EXTENSION IF NOT EXISTS pg_trgm;
