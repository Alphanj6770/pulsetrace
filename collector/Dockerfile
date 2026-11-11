FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY collector/requirements.txt collector/requirements.txt
RUN pip install --no-cache-dir -r collector/requirements.txt

COPY collector collector

RUN useradd --create-home --uid 10001 collector
USER collector

EXPOSE 9000
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:9000/health', timeout=2)"
CMD ["uvicorn", "collector.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "9000"]
