# TraderWizFetcher

TraderWizFetcher is a data ingestion service that fetches market data from a public API (coincap specifically, but it's pretty agnostic to the data, since it saves all the jsons) and publishes it to a Redis channel for further processing

## Features

- Fetches market data from a public API at regular intervals
- Publishes the fetched data to a Redis channel
- Uses PostgreSQL to store fetched data for persistence
- Implements retry logic with exponential backoff for Redis connections
- Asynchronous operations for efficient data handling

## Requirements

Technically only docker, otherwise python 3.13, PostgreSQL, and Redis

## Installation

1. Clone the repo and setup the .env file based on .env.example
2. Run `docker-compose up`
3. Profit
