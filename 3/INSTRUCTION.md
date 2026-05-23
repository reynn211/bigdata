## 1. Запустить все контейнеры

```bash
docker-compose up -d
```

## 2. Наблюдение за процессом

Producer отправляет данные в Kafka автоматически при запуске:

```bash
docker logs -f producer
```

Flink-задание запускается автоматически после инициализации:

```bash
docker logs -f flink-job
```

## 3. Проверка результатов

Подключиться к PostgreSQL и проверить данные:

```bash
docker exec -it postgres psql -U admin -d petstore
```

```sql
SELECT COUNT(*) FROM fact_sales;
SELECT COUNT(*) FROM dim_customer;
SELECT COUNT(*) FROM dim_seller;
SELECT COUNT(*) FROM dim_product;
SELECT COUNT(*) FROM dim_store;
SELECT COUNT(*) FROM dim_supplier;
```

Пример аналитического запроса:

```sql
SELECT
    dc.country,
    COUNT(*) AS total_sales,
    SUM(fs.sale_total_price) AS revenue
FROM fact_sales fs
JOIN dim_customer dc ON fs.customer_id = dc.customer_id
GROUP BY dc.country
ORDER BY revenue DESC
LIMIT 10;
```

## 4. Flink Web UI

Доступен по адресу: [http://localhost:8081](http://localhost:8081)

![image](https://litter.catbox.moe/plezpy.png)

## 5. Остановка

```bash
docker-compose down
```

Для полной очистки:

```bash
docker-compose down -v
```