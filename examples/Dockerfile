# Image for both example services; docker-compose.yml picks the app via `command`.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY examples/requirements.txt examples/requirements.txt
RUN pip install --no-cache-dir -r examples/requirements.txt

# Install the SDK from this repository, exactly as a user would install the package.
COPY pyproject.toml README.md LICENSE ./
COPY src src
RUN pip install --no-cache-dir .

COPY examples examples

RUN useradd --create-home --uid 10001 app
USER app
WORKDIR /home/app/work
ENV PYTHONPATH=/app
