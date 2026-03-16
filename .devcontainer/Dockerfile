# Base image: Microsoft Dev Containers Python 3.13 (used by GitHub Codespaces)
FROM mcr.microsoft.com/devcontainers/python:3.13-bullseye

# OCI labels for GitHub Container Registry
LABEL org.opencontainers.image.source=https://github.com/nordquant/complete-dbt-bootcamp-zero-to-hero
LABEL org.opencontainers.image.description="dbt Zero to Hero Development Container"

# Install UV as root into /usr/local/bin
ENV UV_INSTALL_DIR="/usr/local/bin"
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
    && chmod a+rx /usr/local/bin/uv /usr/local/bin/uvx

# Tell UV to use system Python (no virtualenv)
ENV UV_SYSTEM_PYTHON=1

# Copy project files and install dependencies as root
COPY pyproject.toml uv.lock /workspaces/dev-repo/
WORKDIR /workspaces/dev-repo
RUN uv pip install -r pyproject.toml

# Grant vscode user write access to site-packages (for postCreateCommand)
RUN chown -R vscode:vscode /usr/local/lib/python3.13/site-packages/

# Switch to vscode user for runtime
USER vscode
