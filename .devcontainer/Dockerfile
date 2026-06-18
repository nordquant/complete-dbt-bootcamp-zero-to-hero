# Base image: Microsoft Dev Containers Python 3.13 (used by GitHub Codespaces)
FROM mcr.microsoft.com/devcontainers/python:3.13-bullseye

# OCI labels for GitHub Container Registry
LABEL org.opencontainers.image.source=https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero
LABEL org.opencontainers.image.description="dbt Zero to Hero Development Container"

# uv configuration:
# - Install the uv CLI to /usr/local/bin (root-owned, on PATH for all users).
# - Treat /opt/venv as the project environment so `uv sync` / `uv add` / `uv run`
#   manage that one location, owned by vscode. Avoids a recursive chown of
#   /usr/local (which would duplicate ~3 GB of git-core etc. into a new layer).
# - Put /opt/venv/bin on PATH so installed entry points (dbt, dagster-dbt,
#   dagster-webserver, ...) are visible to every shell, including VSCode terminals.
ENV UV_INSTALL_DIR=/usr/local/bin \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PATH=/opt/venv/bin:$PATH

RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
 && chmod a+rx /usr/local/bin/uv /usr/local/bin/uvx \
 && install -d -o vscode -g vscode /opt/venv \
 && printf 'export PATH="/opt/venv/bin:$PATH"\n' > /etc/profile.d/zz-uv-venv.sh

# Install project deps as vscode into /opt/venv from the lockfile, so the
# baked-in install matches what `uv sync --frozen` expects at runtime
# (postCreate becomes a true no-op when the lock hasn't drifted).
COPY --chown=vscode:vscode pyproject.toml uv.lock /workspaces/dev-repo/
USER vscode
WORKDIR /workspaces/dev-repo
RUN uv sync --frozen --no-install-project
