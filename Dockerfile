FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PROCESSING_CONFIG=/config/server.json HOME=/tmp
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && groupadd -g 10001 processing && useradd -u 10001 -g processing -M processing
COPY app.py store.py files.py feedback.py scheduler.py start.py legacy.py manage.py ./
COPY static ./static
COPY templates ./templates
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/healthz',timeout=3)"
CMD ["python", "start.py"]
