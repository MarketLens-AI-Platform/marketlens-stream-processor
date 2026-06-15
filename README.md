# MarketLens Stream Processor

PySpark application for real-time processing of market data from Kafka to S3 Parquet.

## Features
- Consumes from Kafka topic `raw-market-data`.
- Performs schema validation.
- Writes to AWS S3 in Parquet format with checkpointing.

## Deployment
Deployed as a Kubernetes Deployment using the `marketlens-sa` ServiceAccount for IRSA access to S3.
