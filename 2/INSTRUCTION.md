## Шаг 1: Запустить Docker Compose

```bash
docker-compose up -d --build
```

- **PostgreSQL** (5432) - исходные данные и звёздная схема
- **ClickHouse** (8123, 9000) - отчёты
- **Cassandra** (9042)
- **Neo4j** (7474, 7687)
- **MongoDB** (27017)
- **Valkey** (6379)
- **Spark Master** (8080, 7077)
- **Spark Worker**

При старте PostgreSQL:
1. Создаёт таблицу `raw_data`
2. Загружает все 10 CSV файлов из `исходные данные/`

## Шаг 2: Дождаться готовности сервисов

Убедитесь, что все контейнеры запущены:

```bash
docker-compose ps
```

Проверьте, что данные загружены в PostgreSQL:

```bash
docker exec -it postgres psql -U admin -d petstore -c "SELECT COUNT(*) FROM raw_data;"
```

## Шаг 3: Создание звёздной схемы

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar \
  /opt/spark-apps/star_schema.py
```

После выполнения в PostgreSQL будут созданы таблицы:
- `dim_country`, `dim_brand`, `dim_category` (под-измерения)
- `dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier` (измерения)
- `fact_sales` (факт)

Проверка:

```bash
docker exec -it postgres psql -U admin -d petstore -c "\dt"
docker exec -it postgres psql -U admin -d petstore -c "SELECT COUNT(*) FROM fact_sales;"
```

## Шаг 4: Создание отчётов в ClickHouse

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar,/opt/spark-jars/clickhouse-jdbc-0.6.0-patch5-shaded.jar \
  /opt/spark-apps/clickhouse.py
```

## Шаг 5: Проверка отчётов в ClickHouse

```bash
docker exec -it clickhouse clickhouse-client --password clickhouse
```

Примеры запросов:

```sql
-- Топ-10 продуктов по продажам
SELECT product_name, total_quantity_sold, total_revenue
FROM report_product_sales
ORDER BY rank_by_sales
LIMIT 10;

-- Топ-10 клиентов по сумме покупок
SELECT customer_first_name, customer_last_name, total_spend
FROM report_customer_sales
ORDER BY rank_by_spend
LIMIT 10;

-- Месячные тренды продаж
SELECT year_month, monthly_revenue, monthly_orders, avg_order_size
FROM report_time_sales
ORDER BY sale_year, sale_month;

-- Топ-5 магазинов по выручке
SELECT store_name, store_city, total_revenue
FROM report_store_sales
ORDER BY rank_by_revenue
LIMIT 5;

-- Топ-5 поставщиков по выручке
SELECT supplier_name, total_revenue, avg_product_price
FROM report_supplier_sales
ORDER BY rank_by_revenue
LIMIT 5;

-- Продукты с наивысшим рейтингом
SELECT product_name, avg_rating, total_sales_volume, total_reviews
FROM report_product_quality
ORDER BY rank_by_rating_desc
LIMIT 10;

-- Продукты с наибольшим количеством отзывов
SELECT product_name, total_reviews, avg_rating
FROM report_product_quality
ORDER BY rank_by_reviews
LIMIT 10;
```

## Шаг 6: Создание отчётов в Cassandra

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar \
  --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0 \
  /opt/spark-apps/cassandra_etl.py
```

## Шаг 7: Проверка отчётов в Cassandra

```bash
docker exec -it cassandra cqlsh
```

Примеры запросов:

```sql
USE reports;

-- Все таблицы
DESCRIBE TABLES;

-- Топ-10 продуктов по продажам (из партиции)
SELECT product_name, total_quantity_sold, total_revenue
FROM report_product_sales
LIMIT 10;

-- Топ-10 клиентов по сумме покупок
SELECT customer_first_name, customer_last_name, total_spend
FROM report_customer_sales
LIMIT 10;

-- Месячные тренды продаж
SELECT year_month, monthly_revenue, monthly_orders, avg_order_size
FROM report_time_sales;

-- Магазины по выручке
SELECT store_name, store_city, total_revenue
FROM report_store_sales
LIMIT 5;

-- Поставщики по выручке
SELECT supplier_name, total_revenue, avg_product_price
FROM report_supplier_sales
LIMIT 5;

-- Продукты с наивысшим рейтингом
SELECT product_name, avg_rating, total_sales_volume, total_reviews
FROM report_product_quality
LIMIT 10;
```

## Шаг 8: Создание отчётов в Neo4j

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar \
  /opt/spark-apps/neo4j_etl.py
```

## Шаг 9: Проверка отчётов в Neo4j

```bash
docker exec -it neo4j cypher-shell -u neo4j -p neo4j_pass
```

Примеры запросов:

```cypher
// Топ-10 продуктов по продажам
MATCH (n:ReportProductSales) RETURN n.product_name, n.total_quantity_sold, n.total_revenue ORDER BY n.rank_by_sales LIMIT 10;

// Топ-10 клиентов
MATCH (n:ReportCustomerSales) RETURN n.customer_first_name, n.customer_last_name, n.total_spend ORDER BY n.rank_by_spend LIMIT 10;

// Месячные тренды
MATCH (n:ReportTimeSales) RETURN n.year_month, n.monthly_revenue, n.monthly_orders ORDER BY n.sale_year, n.sale_month;
```

## Шаг 10: Создание отчётов в MongoDB

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /opt/spark-apps/mongodb.py
```

## Шаг 11: Проверка отчётов в MongoDB

```bash
docker exec -it mongodb mongosh -u admin -p admin123 --authenticationDatabase admin
```

Примеры запросов:

```javascript
use reports

// Все коллекции
show collections

// Топ-10 продуктов по продажам
db.report_product_sales.find({}, {product_name: 1, total_quantity_sold: 1, total_revenue: 1}).sort({rank_by_sales: 1}).limit(10)

// Топ-10 клиентов
db.report_customer_sales.find({}, {customer_first_name: 1, customer_last_name: 1, total_spend: 1}).sort({rank_by_spend: 1}).limit(10)
```

## Шаг 12: Создание отчётов в Valkey

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --jars /opt/spark-jars/postgresql-42.7.1.jar \
  /opt/spark-apps/valkey.py
```

## Шаг 13: Проверка отчётов в Valkey

```bash
docker exec -it valkey valkey-cli
```

Примеры команд:

```
# Количество ключей по каждому отчёту
KEYS report:product_sales:*
KEYS report:customer_sales:*
KEYS report:time_sales:*

# Топ-5 продуктов по рангу (через sorted set)
ZRANGE report:product_sales:_index 0 4

# Данные конкретного продукта
HGETALL report:product_sales:1

# Данные конкретного клиента
HGETALL report:customer_sales:1
```

## Шаг 14: Остановка

```bash
docker-compose down
```

Для удаления данных (volumes):

```bash
docker-compose down -v
```