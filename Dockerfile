FROM python:3.13-slim

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    git \
    redis-server \
    postgresql \
    postgresql-client \
    supervisor \
    build-essential \
    ca-certificates \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Node.js (minimal, only needed as Claude Code runtime)
RUN curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

# uv
RUN curl -LsSf https://astral.sh/uv/install.sh | sh
ENV PATH="/root/.local/bin:$PATH"

# Claude Code
RUN npm install -g @anthropic-ai/claude-code

# Postgres init
RUN PG_VERSION=$(ls /usr/lib/postgresql/) && \
    echo "${PG_VERSION}" > /etc/pg_version
USER postgres
RUN PG_VERSION=$(cat /etc/pg_version) && \
    /usr/lib/postgresql/${PG_VERSION}/bin/initdb -D /var/lib/postgresql/data
USER root

# Create non-root dev user
RUN useradd -m -s /bin/bash dev && \
    echo "dev ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

# Install uv for dev user too
RUN su - dev -c "curl -LsSf https://astral.sh/uv/install.sh | sh"

COPY supervisord.conf /etc/supervisor/conf.d/supervisord.conf
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /workspace
CMD ["/entrypoint.sh"]
