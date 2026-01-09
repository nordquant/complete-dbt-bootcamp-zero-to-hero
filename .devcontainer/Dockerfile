# Base image: Microsoft Dev Containers Python 3.13 (used by GitHub Codespaces)
FROM mcr.microsoft.com/devcontainers/python:3.13-bullseye

# OCI labels for GitHub Container Registry
LABEL org.opencontainers.image.source=https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero
LABEL org.opencontainers.image.description="dbt Zero to Hero Development Container"

# Switch to vscode user for UV installation
USER vscode

# Install UV package manager
RUN curl -LsSf https://astral.sh/uv/install.sh | sh

# Add UV to PATH
ENV PATH="/home/vscode/.local/bin:$PATH"

# Copy project files for dependency installation
COPY --chown=vscode:vscode pyproject.toml uv.lock /workspaces/dev-repo/

# Create virtual environment and install dependencies using UV
WORKDIR /workspaces/dev-repo
RUN uv sync --frozen || uv sync
