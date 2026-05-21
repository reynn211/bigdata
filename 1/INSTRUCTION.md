## Шаг 1: Запустить Docker Compose

```bash
docker-compose up -d
```

- **PostgreSQL** (5432) - БД `snowflake_lab`, пользователь `postgres` / `postgres`

При первом старте PostgreSQL автоматически выполнит init-скрипты из `init/`:

1. `01_create_source.sql` - создаёт таблицу `raw_data`
2. `02_load_data.sh` - загружает все 10 CSV файлов из `исходные данные/` в `raw_data`
3. `03_ddl.sql` - создаёт таблицы снежинки: справочники (`dim_country`, `dim_brand`, `dim_category`, `dim_pet_category`, `dim_pet_type`, `dim_pet_breed`, `dim_material`), измерения (`dim_customer`, `dim_seller`, `dim_product`, `dim_store`, `dim_supplier`) и факт (`fact_sales`)
4. `04_dml.sql` - заполняет все таблицы снежинки из `raw_data`

## Шаг 2: Дождаться готовности

```bash
docker-compose ps
docker-compose logs -f postgres
```

## Шаг 3: Проверка результата

Войти в psql:

```bash
docker exec -it snowflake_lab psql -U postgres -d snowflake_lab
```

Примеры запросов:

```sql
-- Все таблицы
\dt

-- Исходные данные
SELECT COUNT(*) FROM raw_data;

-- Факт продаж
SELECT COUNT(*) FROM fact_sales;

-- Размеры справочников
SELECT 'country'  AS dim, COUNT(*) FROM dim_country
UNION ALL SELECT 'brand',    COUNT(*) FROM dim_brand
UNION ALL SELECT 'category', COUNT(*) FROM dim_category
UNION ALL SELECT 'customer', COUNT(*) FROM dim_customer
UNION ALL SELECT 'seller',   COUNT(*) FROM dim_seller
UNION ALL SELECT 'product',  COUNT(*) FROM dim_product
UNION ALL SELECT 'store',    COUNT(*) FROM dim_store
UNION ALL SELECT 'supplier', COUNT(*) FROM dim_supplier;
```

## Шаг 4: Остановка

```bash
docker-compose down
```

Для удаления данных (volume `pgdata`)

```bash
docker-compose down -v
```
