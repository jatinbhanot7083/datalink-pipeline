FROM python:3.11-alpine

WORKDIR /app

# wget lives in busybox on alpine — already present, no install needed
COPY app.py /app/app.py

EXPOSE 9000

# stdlib-only, no pip needed. Logs to stdout + /app/logs/webhook.jsonl
CMD ["python", "-u", "/app/app.py"]
