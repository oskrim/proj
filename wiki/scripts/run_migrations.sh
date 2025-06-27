#!/bin/bash

# Migration runner script for Wikipedia Search Engine
# This script applies database migrations in the correct order

set -e  # Exit on any error

# Default values (can be overridden with environment variables)
DB_NAME=${DB_NAME:-"wiki_test"}
DB_USER=${DB_USER:-"wiki_user"}
DB_PASSWORD=${DB_PASSWORD:-"wiki_password"}
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
MIGRATIONS_DIR="$PROJECT_ROOT/migrations"

echo "Running database migrations for Wikipedia Search Engine..."
echo "Database: $DB_NAME on $DB_HOST:$DB_PORT"
echo "User: $DB_USER"
echo ""

# Function to run a migration file
run_migration() {
    local migration_file="$1"
    local migration_name=$(basename "$migration_file")

    echo "Applying migration: $migration_name"

    if [ ! -f "$migration_file" ]; then
        echo "ERROR: Migration file not found: $migration_file"
        exit 1
    fi

    # Run the migration
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -f "$migration_file"

    if [ $? -eq 0 ]; then
        echo "✅ Successfully applied: $migration_name"
    else
        echo "❌ Failed to apply: $migration_name"
        exit 1
    fi
    echo ""
}

# Function to check database connection
check_connection() {
    echo "Testing database connection..."
    if PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1;" > /dev/null 2>&1; then
        echo "✅ Database connection successful"
    else
        echo "❌ Failed to connect to database"
        echo "Please ensure:"
        echo "  1. PostgreSQL is running"
        echo "  2. Database '$DB_NAME' exists"
        echo "  3. User '$DB_USER' has access"
        echo "  4. Credentials are correct"
        echo ""
        echo "Run './scripts/setup_database.sh' if you haven't set up the database yet."
        exit 1
    fi
}

# Function to create migrations table for tracking
create_migrations_table() {
    echo "Creating migrations tracking table..."
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << 'EOF'
CREATE TABLE IF NOT EXISTS schema_migrations (
    id SERIAL PRIMARY KEY,
    migration_name VARCHAR(255) NOT NULL UNIQUE,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
EOF
    echo "✅ Migrations table ready"
    echo ""
}

# Function to check if migration was already applied
is_migration_applied() {
    local migration_name="$1"
    local count=$(PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -tAc "SELECT COUNT(*) FROM schema_migrations WHERE migration_name = '$migration_name'")
    [ "$count" -gt 0 ]
}

# Function to mark migration as applied
mark_migration_applied() {
    local migration_name="$1"
    PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "INSERT INTO schema_migrations (migration_name) VALUES ('$migration_name') ON CONFLICT (migration_name) DO NOTHING;"
}

# Check if migrations directory exists
if [ ! -d "$MIGRATIONS_DIR" ]; then
    echo "ERROR: Migrations directory not found: $MIGRATIONS_DIR"
    exit 1
fi

# Test database connection
check_connection

# Create migrations tracking table
create_migrations_table

# Find all migration files and sort them
migration_files=($(find "$MIGRATIONS_DIR" -name "*.sql" -type f | sort))

if [ ${#migration_files[@]} -eq 0 ]; then
    echo "No migration files found in $MIGRATIONS_DIR"
    exit 0
fi

echo "Found ${#migration_files[@]} migration file(s):"
for file in "${migration_files[@]}"; do
    echo "  - $(basename "$file")"
done
echo ""

# Apply migrations
applied_count=0
skipped_count=0

for migration_file in "${migration_files[@]}"; do
    migration_name=$(basename "$migration_file")

    if is_migration_applied "$migration_name"; then
        echo "⏭️  Skipping already applied migration: $migration_name"
        ((skipped_count++))
    else
        run_migration "$migration_file"
        mark_migration_applied "$migration_name"
        ((applied_count++))
    fi
done

echo "Migration summary:"
echo "  ✅ Applied: $applied_count"
echo "  ⏭️  Skipped: $skipped_count"
echo "  📁 Total: ${#migration_files[@]}"
echo ""

# Show current schema version
echo "Current migrations status:"
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "SELECT migration_name, applied_at FROM schema_migrations ORDER BY applied_at;"

echo ""
echo "🎉 All migrations completed successfully!"
