## 1. Запуск контейнеров

```bash
docker-compose up -d
```

## 2. Проверка загрузки данных

Проверка ClickHouse:
```bash
docker exec -it clickhouse clickhouse-client --query "SELECT count(*) FROM default.raw_data"
```

Проверка PostgreSQL:
```bash
docker exec -it postgres psql -U postgres -d trino_lab -c "SELECT count(*) FROM raw_data"
```

## 3. Подключение к Trino

```bash
docker exec -it trino trino
```

## 4. Проверка каталогов в Trino

```sql
SHOW CATALOGS;
SHOW SCHEMAS FROM clickhouse;
SHOW SCHEMAS FROM postgresql;
SELECT count(*) FROM clickhouse.default.raw_data;
SELECT count(*) FROM postgresql.public.raw_data;
```

## 5. Создание star-схемы

```bash
docker exec -i trino trino < trino/01_create_star_schema.sql
```

## 6. Создание отчётов

```bash
docker exec -i trino trino < trino/02_create_reports.sql
```

## 7. Просмотр результатов

```bash
docker exec -it trino trino
```

```sql
-- Топ-10 продуктов по выручке
SELECT * FROM clickhouse.default.report_product_sales ORDER BY product_rank;

-- Топ-10 клиентов по расходам
SELECT * FROM clickhouse.default.report_customer_sales ORDER BY customer_rank;

-- Временные тренды продаж
SELECT * FROM clickhouse.default.report_time_sales ORDER BY sale_year, sale_month;

-- Топ-5 магазинов
SELECT * FROM clickhouse.default.report_store_sales ORDER BY store_rank;

-- Топ-5 поставщиков
SELECT * FROM clickhouse.default.report_supplier_sales ORDER BY supplier_rank;

-- Качество продуктов (рейтинг vs продажи)
SELECT * FROM clickhouse.default.report_product_quality ORDER BY rating DESC LIMIT 20;
```

## Остановка

```bash
docker-compose down
```

Для полной очистки (с удалением данных):
```bash
docker-compose down -v
```
