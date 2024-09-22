import asyncio
from prefect import flow, task, get_run_logger
from prefect.task_runners import ConcurrentTaskRunner
import pandas as pd
import requests
import os
import time
from threading import Semaphore
from datetime import timedelta
import statistics
from dotenv import load_dotenv


load_dotenv(override=True)


csv_file_path = os.getenv("CSV_FILE_PATH")
bot_token = os.getenv("BOT_TOKEN")
username = os.getenv("USERNAME")

LATENCY_THRESHOLD = 5
MAX_PARALLEL_REQUESTS = 5
MIN_PARALLEL_REQUESTS = 1

current_parallel_requests = MIN_PARALLEL_REQUESTS
worker_semaphore = Semaphore(current_parallel_requests)
latencies = []


@task(
    retries=3,
    retry_delay_seconds=5,
    cache_key_fn=lambda task, kwargs: f"fetch_api_data-{kwargs['ticker']}-{kwargs['delay']}",
    cache_expiration=timedelta(days=1),
)
async def fetch_api_data_with_delay(ticker, delay=12) -> dict:
    """Получение данных с внешнего API по тикеру с отложенным выполнением"""
    
    logger = get_run_logger()
    
    global worker_semaphore
    with worker_semaphore:
        start_time = time.time()
        
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={ticker}&apikey=demo"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        if "Time Series (Daily)" not in data:
            logger.warning(f"No 'Time Series (Daily)' data found for ticker {ticker}")
            return None
        
        end_time = time.time()
        latency = end_time - start_time
        latencies.append(latency)
        
        logger.info(f"Data successfully fetched for ticker {ticker} in {latency:.2f} seconds")
        
        await asyncio.sleep(delay)
        
        return data


@task
async def process_data_async(data) -> pd.DataFrame:
    """Асинхронная обработка данных с помощью pandas"""
    
    if data is None:
        return pd.DataFrame()

    df = pd.DataFrame.from_dict(data.get("Time Series (Daily)", {}), orient="index")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "Date"}, inplace=True)
    return df


@task
def save_to_json(df, ticker) -> str:
    """Сохранение обработанных данных в JSON"""
    
    if df.empty:
        get_run_logger().warning(f"Dataframe for ticker {ticker} is empty, skipping saving.")
        return None

    output_dir = "output"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{ticker}.json")
    df.to_json(output_path, orient="records")

    get_run_logger().info(f"Data for {ticker} saved to {output_path}")
    return output_path


@task
def send_telegram_notification(bot_token, chat_id, message) -> None:
    """Отправка уведомления в Telegram чат"""
    
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {"chat_id": chat_id, "text": message}
    response = requests.get(url, params=params)
    if response.status_code == 200:
        get_run_logger().info("Notification sent successfully.")
    else:
        get_run_logger().warning("Failed to send notification.")


@task
def adjust_semaphore() -> None:
    """Динамическая регулировка количества параллельных потоков"""
    
    global current_parallel_requests, worker_semaphore
    logger = get_run_logger()

    if len(latencies) < 5:
        return

    avg_latency = statistics.mean(latencies[-5:])
    logger.info(f"Current average latency: {avg_latency:.2f} seconds")

    if avg_latency > LATENCY_THRESHOLD and current_parallel_requests > MIN_PARALLEL_REQUESTS:
        current_parallel_requests -= 1
        logger.info(f"Reducing parallel requests to {current_parallel_requests}")
    elif avg_latency <= LATENCY_THRESHOLD and current_parallel_requests < MAX_PARALLEL_REQUESTS:
        current_parallel_requests += 1
        logger.info(f"Increasing parallel requests to {current_parallel_requests}")

    worker_semaphore = Semaphore(current_parallel_requests)


@task
def get_chat_id_by_username(username, bot_token) -> int:
    """Получение chat_id по username"""
    
    url = f"https://api.telegram.org/bot{bot_token}/getUpdates"
    response = requests.get(url)
    response.raise_for_status()

    updates = response.json().get("result", [])
    for update in updates:
        message = update.get("message", {})
        if message.get("chat", {}).get("username", "").lower() == username.lower():
            return message.get("chat", {}).get("id")

    raise ValueError(f"Chat ID для username {username} не найден.")


@flow(name="Process Ticker Flow with Delay")
async def process_ticker_flow_with_delay(ticker, delay) -> None:
    """Flow для обработки тикера с отложенным выполнением"""
    
    data = await fetch_api_data_with_delay(ticker, delay=delay)
    df = await process_data_async(data)
    save_to_json(df, ticker)
    adjust_semaphore()


@flow(name="Data Processing Flow with Delays", task_runner=ConcurrentTaskRunner())
async def main_flow_with_delays(csv_file_path, bot_token, username, delay) -> None:
    """Основной flow для обработки данных с задержкой"""
    
    chat_id = get_chat_id_by_username(username, bot_token)
    logger = get_run_logger()

    df = pd.read_csv(csv_file_path, delimiter=";")
    tickers = df["symbol"].unique()

    tasks = [process_ticker_flow_with_delay(ticker, delay) for ticker in tickers]
    await asyncio.gather(*tasks)

    send_telegram_notification(bot_token, chat_id, "FFlow execution completed")
    logger.info("Flow execution completed.")


if __name__ == "__main__":
    asyncio.run(main_flow_with_delays(csv_file_path, bot_token, username, delay=12))
