FROM python:3.11-slim

# Системные зависимости для pyodbc (ODBC Driver for SQL Server)
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg2 \
        unixodbc-dev \
        apt-transport-https \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" \
        > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get purge -y --auto-remove curl gnupg2 apt-transport-https \
    && rm -rf /var/lib/apt/lists/*

# Рабочая директория
WORKDIR /app

# Зависимости (кэшируется при неизменном requirements.txt)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Код приложения
COPY config.py etl.py orchestrator.py ./
COPY pipeline/ pipeline/
COPY tests/ tests/

# Директории для данных (монтируются как volumes)
RUN mkdir -p /data/raw /data/interim /data/preproc_parquet /data/result /data/history /logs

# Переменные окружения (переопределяются в docker-compose или при запуске)
ENV ETL_PROJECT_ROOT=/app \
    PYTHONUNBUFFERED=1 \
    PYTHONIOENCODING=utf-8

# Точка входа — по умолчанию запуск ETL
ENTRYPOINT ["python"]
CMD ["etl.py"]
