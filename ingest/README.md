# Ingest Module for TraderWizFetcher

This module is responsible for ingesting data into the TraderWizFetcher system. It includes scripts and configurations necessary for data extraction, transformation, and loading (ETL) processes. Nothing fancy much, since currently it doesn't need to ingest anything complex.

I purposefully decided to avoid having too much dependencies and complexity (migrations, (de-)serialization, etc.) in here, since it is supposed to be a simple data ingestion module

## Running the Ingest Worker

I would recommend building and running the dockerfile in the root:

```bash
docker build -t traderwizfetcher-ingest .
docker run --env-file .env traderwizfetcher-ingest
```
