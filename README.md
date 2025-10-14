# Real-time Event Ingestion Pipeline from Historical Data

This project demonstrates a common data engineering task: building a reliable pipeline to process a high-volume stream of events. The core challenge is to simulate a realistic, real-time stream of user clicks using a large, static, historical dataset and ingest this data into a structured data lake on AWS S3.

The pipeline is designed to be fault-tolerant, scalable, and ready for downstream analytics.

----

## The Core Task & Dataset

The primary goal is to *emulate real-time user interactions* from a historical dataset and build a pipeline to capture these events.

The project uses the *OLX Poland Job Postings* dataset, which contains *65.5 million events* from *3.3 million users* interacting with over 185,000 job ads. This large dataset is used to generate a continuous and realistic stream of data.

----

## Written Components & Architecture

The pipeline consists of two main custom components written in Python, orchestrated by a Kafka cluster.

1. Producer (`data_producer.py`)

    - *Task*: Reads the massive OLX events CSV file in chunks using the Polars library.

    - *Function*: It acts as the real-time simulator. It takes the historical data and publishes it to a Kafka topic row-by-row, introducing small delays to mimic a live stream of user clicks on a website.

2. Consumer (`data_consumer.py`)

    - *Task*: Subscribes to the Kafka topic to receive the stream of click events.

    - *Function*: Its main job is to batch and store the data efficiently. It gathers events until either a certain number of messages are collected (e.g., 1,000) or a time limit is reached (e.g., 60 seconds). It then uploads this batch to an AWS S3 bucket.

    - *Key Feature*: The consumer stores data in a partitioned format (`/year=YYYY/month=MM/day=DD/hour=HH/`), which is a critical best practice for optimizing queries in a data lake. It also manages Kafka offsets manually to prevent data loss if the service fails.

----

## Technology Stack

- *Data Streaming*: Apache Kafka (running in a 2-node, Zookeeper-less KRaft cluster via Docker).

- *Backend Logic*: Python 3.10+.
 
- *Data Handling*: Polars for high-performance reading of the large CSV file.
 
- *AWS Integration*: Boto3 library for uploading data to S3.
 
- *Infrastructure*: Docker & Docker Compose for running the entire environment locally.
 
- *Monitoring*: Kafka UI for observing topics and data flow.

----

## Future Use Cases

The data pipeline populates an S3 data lake. This structured data can then be used for:

- *Business Intelligence*: Building dashboards to analyze user engagement with job ads.

- *Machine Learning*: Training recommendation models to suggest relevant jobs to users.

- *Data Analysis*: Performing large-scale analysis on user behavior patterns using tools like AWS Athena, Spark, or Trino.

----

## TODO & Future Improvements

Here is a list of potential features and improvements for the project:

- [ ] Retry Logic: Implement retry mechanisms for both producer (Kafka) and consumer (S3) operations to handle transient network errors.

- [ ] Dead Letter Queue (DLQ): Route unprocessable messages to a separate topic for inspection instead of crashing the consumer.

- [ ] Schema Enforcement: Integrate a Schema Registry with a format like Avro to ensure data quality and contract between services.

- [ ] Testing: Add unit tests for business logic and an integration test suite using a mock S3 service (e.g., moto).

- [ ] CI/CD Pipeline: Set up a GitHub Actions workflow to automate testing on every push and pull request.

- [ ] Dockerize Applications: Create Dockerfiles for the Python producer and consumer to simplify deployment and scaling.

- [ ] Metrics & Monitoring: Expose key application metrics (e.g., messages processed/sec, errors) using Prometheus for better observability.

----

## Author

**Illia Stetsenko**  
[illia.stetsenko.work@gmail.com](mailto:illia.stetsenko.work@gmail.com)
