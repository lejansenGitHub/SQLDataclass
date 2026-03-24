#!/bin/bash
set -e

export PG_MAJOR=$(cat /etc/pg_version)

# Start supervisor (postgres + redis) in background
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf &

# Wait for Postgres to be ready
echo "Waiting for Postgres..."
until su postgres -c "pg_isready" > /dev/null 2>&1; do
    sleep 0.5
done

# Create the database if it doesn't exist
su postgres -c "psql -tc \"SELECT 1 FROM pg_database WHERE datname = 'sqldataclass'\" | grep -q 1" || \
    su postgres -c "createdb sqldataclass"

echo "Waiting for Redis..."
until redis-cli ping > /dev/null 2>&1; do
    sleep 0.5
done

# Copy Claude auth to dev user's home (owned by dev, writable)
mkdir -p /home/dev/.claude/sessions /home/dev/.claude/statsig
cp -a /tmp/claude-sessions/* /home/dev/.claude/sessions/ 2>/dev/null || true
cp -a /tmp/claude-statsig/* /home/dev/.claude/statsig/ 2>/dev/null || true
chown -R dev:dev /home/dev/.claude

# Give dev user ownership of workspace
chown -R dev:dev /workspace

# Install project dependencies as dev user
su - dev -c "cd /workspace && /home/dev/.local/bin/uv sync"

echo ""
echo "=== Sandbox ready ==="
echo "  Postgres: postgresql://postgres@localhost:5432/sqldataclass"
echo "  Redis:    redis://localhost:6379"
echo ""
echo "  Start Claude Code:  claude --dangerously-skip-permissions"
echo ""

exec su - dev -c "cd /workspace && exec bash"
