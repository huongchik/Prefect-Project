FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY . .

RUN pip install --upgrade pip && \
    pip install -r requirements.txt

ENV CSV_FILE_PATH=/app/data/CSV.csv
ENV BOT_TOKEN=${BOT_TOKEN}
ENV USERNAME=${USERNAME}

RUN prefect deploy -p flows/data_processing_flow.py:main_flow -n "Data Processing Flow" --pool default

CMD prefect server start & sleep 10 && python flows/data_processing_flow.py
