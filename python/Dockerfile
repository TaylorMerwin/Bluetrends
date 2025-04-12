FROM python:3.9-slim

WORKDIR /home/app

# Install system dependencies needed for Poetry
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Install Poetry
RUN curl -sSL https://install.python-poetry.org | python3 -

# Add Poetry to PATH
ENV PATH="/root/.local/bin:$PATH"

# Copy only Poetry configuration files first to leverage Docker cache
COPY pyproject.toml poetry.lock* ./

# Configure Poetry not to create a virtual environment and install dependencies globally
RUN poetry config virtualenvs.create false \
    && poetry install --no-interaction --no-ansi

# Now copy your actual project files
COPY . .

# Set default command (adjust as necessary)
CMD ["python", "src/ingestion/jetstream_producer.py"]