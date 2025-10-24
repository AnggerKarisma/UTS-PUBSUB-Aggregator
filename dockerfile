FROM python:3.11-slim

WORKDIR /app
# create non-root user
RUN adduser --disabled-password --gecos '' appuser && chown -R appuser:appuser /app

USER appuser

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY README.md ./
COPY tests/ ./tests/
EXPOSE 8080

ENV PATH="/home/appuser/.local/bin:${PATH}"

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/stats')" || exit 1

CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8080", "--workers", "1"]