FROM python:3.12-slim

WORKDIR /app

# Install dependencies
RUN pip install poetry
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false && \
    poetry install --no-interaction --no-ansi

# Copy application code
COPY . .

# Set command to run
ENTRYPOINT ["poetry", "run", "store-calc"]
