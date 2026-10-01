FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
COPY burst.py .
EXPOSE 8000
# single worker: async I/O + DB-side atomicity; scale by running more containers (state lives in Postgres)
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1 --no-access-log"]
