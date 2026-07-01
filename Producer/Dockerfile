FROM python:3.11-slim

WORKDIR /app

COPY ticket_producer.py .

RUN pip install kafka-python

CMD ["python", "ticket_producer.py"]