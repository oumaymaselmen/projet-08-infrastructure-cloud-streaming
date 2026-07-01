FROM python:3.11-slim

WORKDIR /app

COPY ticket_consumer.py .

RUN pip install kafka-python pandas

CMD ["python", "ticket_consumer.py"]