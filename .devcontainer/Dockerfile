# Base image: Microsoft Dev Containers Python 3.13 (used by GitHub Codespaces)
FROM mcr.microsoft.com/devcontainers/python:3.13-bullseye

# OCI labels for GitHub Container Registry
LABEL org.opencontainers.image.source=https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero
LABEL org.opencontainers.image.description="dbt Zero to Hero Development Container"

# Install UV package manager (as root, then make it available globally)
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && ln -sf /root/.local/bin/uv /usr/local/bin/uv \
    && ln -sf /root/.local/bin/uvx /usr/local/bin/uvx

# Tell UV to install into system Python (no virtualenv)
ENV UV_SYSTEM_PYTHON=1

# Copy project files for dependency installation
COPY pyproject.toml uv.lock /workspaces/dev-repo/

# Install dependencies into system site-packages
WORKDIR /workspaces/dev-repo
RUN uv pip install -r pyproject.toml

# Switch to vscode user for runtime
USER vscode
