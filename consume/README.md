# Consume Module for TraderWizFetcher

This module is responsible for consuming market data from an ingestion queue and processing it for further use. It connects to a Redis server to subscribe to a specific channel where market data messages are published. The module is designed to handle incoming messages asynchronously, ensuring efficient processing and minimal latency.
Additionally it has endpoints to fetch the data from the DB to collect previous data.

## Running the Worker

It's better to use the dockerfile to build and run it:

```bash
docker build -t traderwizfetcher-consume -f consume/Dockerfile .
docker run -d --name traderwizfetcher-consume --env-file .env traderwiz
```
