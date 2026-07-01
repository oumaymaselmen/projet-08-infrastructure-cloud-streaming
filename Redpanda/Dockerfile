FROM redpandadata/redpanda:latest

EXPOSE 9092 9644

CMD ["redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "1G", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", "0.0.0.0:9092", "--advertise-kafka-addr", "redpanda:9092"]