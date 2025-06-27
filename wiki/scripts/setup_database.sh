#!/bin/bash

# Database setup script for Wikipedia Search Engine
# This script creates a PostgreSQL user and database for the project

set -e  # Exit on any error

# Default values (can be overridden with environment variables)
DB_NAME=${DB_NAME:-"wiki_test"}
DB_USER=${DB_USER:-"wiki_user"}
DB_PASSWORD=${DB_PASSWORD:-"wiki_password"}
DB_HOST=${DB_HOST:-"localhost"}
DB_PORT=${DB_PORT:-"5432"}
POSTGRES_ADMIN_USER=${POSTGRES_ADMIN_USER:-"postgres"}

echo "Setting up PostgreSQL database for Wikipedia Search Engine..."
echo "Database: $DB_NAME"
echo "User: $DB_USER"
echo "Host: $DB_HOST:$DB_PORT"

# Function to run SQL as admin user
run_sql_as_admin() {
    local sql="$1"
    PGPASSWORD="${POSTGRES_ADMIN_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_ADMIN_USER" -d postgres -c "$sql"
}

# Function to check if database exists
database_exists() {
    local db_name="$1"
    PGPASSWORD="${POSTGRES_ADMIN_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_ADMIN_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$db_name'" | grep -q 1
}

# Function to check if user exists
user_exists() {
    local username="$1"
    PGPASSWORD="${POSTGRES_ADMIN_PASSWORD}" psql -h "$DB_HOST" -p "$DB_PORT" -U "$POSTGRES_ADMIN_USER" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$username'" | grep -q 1
}

# Prompt for postgres admin password if not set
if [ -z "$POSTGRES_ADMIN_PASSWORD" ]; then
    read -s -p "Enter PostgreSQL admin password for user '$POSTGRES_ADMIN_USER': " POSTGRES_ADMIN_PASSWORD
    echo
fi

echo "Connecting to PostgreSQL..."

# Create user if it doesn't exist
if user_exists "$DB_USER"; then
    echo "User '$DB_USER' already exists, skipping user creation."
else
    echo "Creating user '$DB_USER'..."
    run_sql_as_admin "CREATE USER $DB_USER WITH PASSWORD '$DB_PASSWORD';"
fi

# Create database if it doesn't exist
if database_exists "$DB_NAME"; then
    echo "Database '$DB_NAME' already exists, skipping database creation."
else
    echo "Creating database '$DB_NAME'..."
    run_sql_as_admin "CREATE DATABASE $DB_NAME OWNER $DB_USER;"
fi

# Grant necessary privileges
echo "Granting privileges..."
run_sql_as_admin "GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;"
run_sql_as_admin "ALTER USER $DB_USER CREATEDB;"  # Needed for running tests

# Connect to the new database and set up extensions
echo "Setting up database extensions..."
PGPASSWORD="$DB_PASSWORD" psql -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" << EOF
-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";

-- Try to create vector extension (may fail if not installed)
DO \$\$
BEGIN
    CREATE EXTENSION IF NOT EXISTS "vector";
    RAISE NOTICE 'pgvector extension created successfully';
EXCEPTION
    WHEN OTHERS THEN
        RAISE WARNING 'pgvector extension not available. Please install pgvector for vector search functionality.';
        RAISE WARNING 'Installation instructions: https://github.com/pgvector/pgvector#installation';
END
\$\$;
EOF

echo "Database setup completed successfully!"
echo ""
echo "Connection details:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""
echo "You can now run migrations with:"
echo "  ./scripts/run_migrations.sh"
echo ""
echo "Or connect manually with:"
echo "  psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME"
