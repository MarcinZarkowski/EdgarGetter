# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency management
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set working directory
WORKDIR /app

# Copy the project files
COPY pyproject.toml uv.lock ./
COPY main.py ./
COPY db ./db
COPY src ./src

# Install dependencies using uv
# --frozen ensures we use the exact versions from uv.lock
# --no-install-project avoids trying to install the current folder as a package
RUN uv sync --frozen --no-dev --no-install-project

# Ensure the app can find the installed packages
ENV PATH="/app/.venv/bin:$PATH"

# Set environment variables for production
ENV PYTHONUNBUFFERED=1

# Command to run the application
CMD ["python", "main.py"]
