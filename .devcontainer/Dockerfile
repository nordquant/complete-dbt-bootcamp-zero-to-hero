# Base image: Microsoft Dev Containers Python 3.12 (used by GitHub Codespaces)
FROM mcr.microsoft.com/devcontainers/python:3.12-bullseye

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
