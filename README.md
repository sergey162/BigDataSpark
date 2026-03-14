# Анализ больших данных — Лабораторная работа №2
**Тема:** ETL реализованный с помощью Spark.

В данном проекте реализован ETL-пайплайн с использованием Apache Spark. Пайплайн загружает сырые данные из CSV, преобразует их в схему «Снежинка» в PostgreSQL, а затем на её основе строит 6 аналитических витрин (отчетов) в колоночной БД ClickHouse.

## Структура проекта
* `data/` — исходные файлы `mock_data_*.csv`.
* `docker-compose.yml` — конфигурация инфраструктуры (PostgreSQL, ClickHouse, Apache Spark).
* `job1_etl_postgres.py` — PySpark-скрипт трансформации сырых CSV в PostgreSQL (модель «Снежинка»).
* `job2_etl_clickhouse.py` — PySpark-скрипт генерации 6 аналитических отчетов и их загрузки в ClickHouse.

## Инструкция по запуску

### 1. Поднятие инфраструктуры
Запустите базы данных и контейнер со Spark:
```bash
docker-compose up -d

docker exec -it spark_master spark-submit --packages org.postgresql:postgresql:42.6.0 /home/jovyan/work/job1_etl_postgres.py

docker exec -it spark_master spark-submit --packages org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.4.6 /home/jovyan/work/job2_etl_clickhouse.py


В репозитории есть картинка из DBeaver как у меня это все работает локально.