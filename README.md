# Market Service

This repository contains the code for Allokate's Market service. This service is a REST API that serves up stock market pricing data.

# Table of Contents

- [Market Service](#market-service)
- [Table of Contents](#table-of-contents)
- [Overview](#overview)
- [Sickle Script](#sickle-script)
- [Endpoints](#endpoints)

# Overview

This service was broken out from the accounts service to separate code on several boundaries:

- Data privacy boundaries: separating private transactions data and account information from public data allowing data to be stored in separate databases.
- Functionality boundaries: to prevent code related to serving market data of being conflated with accounting code and to reduce dependency bloat of a single service.
- Scalability boundaries: to allow the endpoints for serving market data to be scaled independently of the account endpoints as they are expected to have differing demand rates.

The service provides endpoints for basic market data and a few additional endpoints for assessing the performance of a basket of stocks.

# Sickle Script

The script underpinning a cron job that is used to harvest market pricing data from Yahoo finance. The cron job runs once per day at the close of the trading day and fetching new data for each of the symbols within the database.

# Endpoints

- GET /api/ping
- GET /api/market/prices
- GET /api/market/tickers
- GET /api/market/:ticker
- GET /api/market/performance
