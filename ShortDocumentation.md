# Project Documentation

## 1. Overview
This solution is designed for asynchronous data processing of stock tickers via an external API. It uses **Prefect 3.0** for task orchestration and flow management, while implementing dynamic control over parallel requests based on measured request latency.

## 2. Main Components
- **CSV file with tickers**: Contains stock tickers for which data will be fetched.
- **API for fetching stock data**: Data is retrieved from the Alpha Vantage API.
- **Asynchronous data processing**: Utilizes `asyncio` and **Prefect** to manage concurrent tasks and efficiently process large datasets.
- **Semaphore for concurrency control**: A semaphore is used to dynamically adjust the number of parallel API requests based on latency.
- **Telegram bot**: Sends notifications to a Telegram chat upon task completion.

## 3. Key Decisions
- **Asynchronous data fetching**: Data fetching and processing tasks are managed using Prefect’s asynchronous functions, allowing for parallel API requests and faster execution.
- **Semaphore and dynamic concurrency control**:
  - **Semaphore** controls the number of concurrent API requests, with a minimum of 1 and a maximum of 5 parallel requests.
  - Request latency is monitored, and based on the average latency of the last 5 requests, the semaphore adjusts the number of parallel requests. This prevents API overload and ensures adherence to rate limits.

## 4. Limits and Constraints
- **Latency threshold**: If the average latency exceeds 5 seconds, the number of parallel requests is reduced. If the latency stays below this threshold, the number of concurrent requests is increased.
- **API rate limits**: The solution supports retrying failed requests (up to 3 retries with a 5-second delay), which helps avoid request failures.

## 5. Security
- Environment variables are used to securely store sensitive information, such as the CSV file path, bot token, and username.

## 6. Technologies Used
- **Prefect 3.0** for task orchestration and flow management.
- **Asyncio** for asynchronous operations.
- **Pandas** for data processing and transforming raw API data into DataFrames.
- **Requests** for sending API requests.
- **JSON** for saving the processed data.


