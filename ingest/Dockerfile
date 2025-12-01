FROM python:3.13-slim

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Set environment variables
ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy dependency files
COPY pyproject.toml uv.lock* .python-version ./

# Install dependencies - no freeze because deps are handled on the upper level (it's bad)
RUN uv sync

# Copy the application code into a package structure
COPY . /app/

# Run the application
CMD ["uv", "run", "main.py"]