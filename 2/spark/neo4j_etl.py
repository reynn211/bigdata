from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPERTIES = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "neo4j_pass"


def get_spark():
    return (
        SparkSession.builder
        .appName("ETL_StarSchema_to_Neo4j")
        .getOrCreate()
    )


def read_pg(spark, table):
    return spark.read.jdbc(url=PG_URL, table=table, properties=PG_PROPERTIES)


def get_neo4j_driver():
    from neo4j import GraphDatabase
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def clear_neo4j():
    driver = get_neo4j_driver()
    with driver.session() as session:
        labels = [
            "ReportProductSales", "ReportCustomerSales", "ReportTimeSales",
            "ReportStoreSales", "ReportSupplierSales", "ReportProductQuality"
        ]
        for label in labels:
            session.run(f"MATCH (n:{label}) DETACH DELETE n")
            print(f"  Cleared {label}")
    driver.close()


def write_nodes(rows, label, batch_size=500):
    driver = get_neo4j_driver()
    with driver.session() as session:
        for i in range(0, len(rows), batch_size):
            batch = rows[i:i + batch_size]
            session.run(
                f"UNWIND $batch AS row CREATE (n:{label}) SET n = row",
                batch=batch
            )
    driver.close()
    print(f"  -> {label}: {len(rows)} nodes written to Neo4j")


def df_to_dicts(df):
    from decimal import Decimal
    df = df.na.fill("").na.fill(0)
    def coerce(v):
        if isinstance(v, Decimal):
            return float(v)
        return v
    return [{k: coerce(v) for k, v in row.asDict().items()} for row in df.collect()]


def main():
    spark = get_spark()

    clear_neo4j()

    fact_sales = read_pg(spark, "fact_sales")
    dim_customer = read_pg(spark, "dim_customer")
    dim_seller = read_pg(spark, "dim_seller")
    dim_product = read_pg(spark, "dim_product")
    dim_store = read_pg(spark, "dim_store")
    dim_supplier = read_pg(spark, "dim_supplier")
    dim_country = read_pg(spark, "dim_country")
    dim_brand = read_pg(spark, "dim_brand")
    dim_category = read_pg(spark, "dim_category")

    fact_sales.cache()
    dim_product.cache()

    product_full = (
        dim_product
        .join(dim_category, dim_product.category_id == dim_category.category_id, "left")
        .join(dim_brand, dim_product.brand_id == dim_brand.brand_id, "left")
        .select(
            dim_product.product_id,
            dim_product.product_name,
            dim_category.category_name,
            dim_brand.brand_name,
            dim_product.product_price,
            dim_product.product_rating,
            dim_product.product_reviews,
            dim_product.product_weight,
            dim_product.product_color,
            dim_product.product_size,
            dim_product.product_material,
            dim_product.product_description,
            dim_product.pet_category
        )
    )
    product_full.cache()

    print("\nCreating ReportProductSales...")
    sales_product = fact_sales.join(product_full, "product_id", "left")

    top10_products = (
        sales_product
        .groupBy("product_id", "product_name", "category_name")
        .agg(
            F.sum("sale_quantity").alias("total_quantity_sold"),
            F.sum("sale_total_price").alias("total_revenue"),
            F.avg("product_rating").alias("avg_rating"),
            F.avg("product_reviews").alias("avg_reviews")
        )
        .withColumn("rank_by_sales", F.row_number().over(
            Window.orderBy(F.desc("total_quantity_sold"))))
    )

    revenue_by_category = (
        sales_product
        .groupBy("category_name")
        .agg(
            F.sum("sale_total_price").alias("category_revenue"),
            F.sum("sale_quantity").alias("category_quantity"),
            F.countDistinct("product_id").alias("products_count")
        )
    )

    report_product_sales = (
        top10_products
        .join(revenue_by_category, "category_name", "left")
        .select(
            "product_id", "product_name", "category_name",
            "total_quantity_sold", "total_revenue",
            F.round("avg_rating", 2).alias("avg_rating"),
            F.round("avg_reviews", 0).cast("int").alias("avg_reviews"),
            "rank_by_sales",
            "category_revenue", "category_quantity", "products_count"
        )
        .orderBy("rank_by_sales")
    )
    write_nodes(df_to_dicts(report_product_sales), "ReportProductSales")

    print("Creating ReportCustomerSales...")
    customer_full = (
        dim_customer
        .join(dim_country, dim_customer.country_id == dim_country.country_id, "left")
        .select(
            dim_customer.customer_id,
            dim_customer.customer_first_name,
            dim_customer.customer_last_name,
            dim_customer.customer_age,
            dim_customer.customer_email,
            dim_country.country_name.alias("customer_country")
        )
    )

    sales_customer = fact_sales.join(customer_full, "customer_id", "left")

    customer_stats = (
        sales_customer
        .groupBy(
            "customer_id", "customer_first_name", "customer_last_name",
            "customer_age", "customer_email", "customer_country"
        )
        .agg(
            F.sum("sale_total_price").alias("total_spend"),
            F.count("sale_id").alias("total_orders"),
            F.avg("sale_total_price").alias("avg_check")
        )
        .withColumn("rank_by_spend", F.row_number().over(
            Window.orderBy(F.desc("total_spend"))))
    )

    customers_by_country = (
        sales_customer
        .groupBy("customer_country")
        .agg(F.countDistinct("customer_id").alias("customers_in_country"))
    )

    report_customer_sales = (
        customer_stats
        .join(customers_by_country, "customer_country", "left")
        .select(
            "customer_id", "customer_first_name", "customer_last_name",
            "customer_country", "customer_age",
            F.round("total_spend", 2).alias("total_spend"),
            "total_orders",
            F.round("avg_check", 2).alias("avg_check"),
            "rank_by_spend",
            "customers_in_country"
        )
        .orderBy("rank_by_spend")
    )
    write_nodes(df_to_dicts(report_customer_sales), "ReportCustomerSales")

    print("Creating ReportTimeSales...")
    fact_with_time = (
        fact_sales
        .withColumn("sale_year", F.year("sale_date"))
        .withColumn("sale_month", F.month("sale_date"))
        .withColumn("year_month", F.date_format("sale_date", "yyyy-MM"))
    )

    monthly = (
        fact_with_time
        .groupBy("sale_year", "sale_month", "year_month")
        .agg(
            F.sum("sale_total_price").alias("monthly_revenue"),
            F.sum("sale_quantity").alias("monthly_quantity"),
            F.count("sale_id").alias("monthly_orders"),
            F.avg("sale_total_price").alias("avg_order_size")
        )
    )

    yearly = (
        fact_with_time
        .groupBy("sale_year")
        .agg(F.sum("sale_total_price").alias("yearly_revenue"))
    )

    report_time_sales = (
        monthly
        .join(yearly, "sale_year", "left")
        .select(
            "sale_year", "sale_month", "year_month",
            F.round("monthly_revenue", 2).alias("monthly_revenue"),
            "monthly_quantity", "monthly_orders",
            F.round("avg_order_size", 2).alias("avg_order_size"),
            F.round("yearly_revenue", 2).alias("yearly_revenue")
        )
        .orderBy("sale_year", "sale_month")
    )
    write_nodes(df_to_dicts(report_time_sales), "ReportTimeSales")

    print("Creating ReportStoreSales...")
    store_full = (
        dim_store
        .join(dim_country, dim_store.country_id == dim_country.country_id, "left")
        .select(
            dim_store.store_id,
            dim_store.store_name,
            dim_store.store_city,
            dim_store.store_state,
            dim_country.country_name.alias("store_country")
        )
    )

    sales_store = fact_sales.join(store_full, "store_id", "left")

    store_stats = (
        sales_store
        .groupBy("store_id", "store_name", "store_city", "store_state", "store_country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("total_orders"),
            F.sum("sale_quantity").alias("total_quantity"),
            F.avg("sale_total_price").alias("avg_check")
        )
        .withColumn("rank_by_revenue", F.row_number().over(
            Window.orderBy(F.desc("total_revenue"))))
    )

    sales_by_city = (
        sales_store
        .groupBy("store_city", "store_country")
        .agg(F.sum("sale_total_price").alias("city_revenue"))
    )

    report_store_sales = (
        store_stats
        .join(sales_by_city, ["store_city", "store_country"], "left")
        .select(
            "store_id", "store_name", "store_city", "store_state", "store_country",
            F.round("total_revenue", 2).alias("total_revenue"),
            "total_orders", "total_quantity",
            F.round("avg_check", 2).alias("avg_check"),
            "rank_by_revenue",
            F.round("city_revenue", 2).alias("city_revenue")
        )
        .orderBy("rank_by_revenue")
    )
    write_nodes(df_to_dicts(report_store_sales), "ReportStoreSales")

    print("Creating ReportSupplierSales...")
    supplier_full = (
        dim_supplier
        .join(dim_country, dim_supplier.country_id == dim_country.country_id, "left")
        .select(
            dim_supplier.supplier_id,
            dim_supplier.supplier_name,
            dim_supplier.supplier_city,
            dim_country.country_name.alias("supplier_country")
        )
    )

    sales_supplier = (
        fact_sales
        .join(
            dim_product.select("product_id", "supplier_id", "product_price"),
            "product_id", "left"
        )
        .join(supplier_full, "supplier_id", "left")
    )

    supplier_stats = (
        sales_supplier
        .groupBy("supplier_id", "supplier_name", "supplier_city", "supplier_country")
        .agg(
            F.sum("sale_total_price").alias("total_revenue"),
            F.count("sale_id").alias("total_orders"),
            F.avg("product_price").alias("avg_product_price"),
            F.sum("sale_quantity").alias("total_quantity")
        )
        .withColumn("rank_by_revenue", F.row_number().over(
            Window.orderBy(F.desc("total_revenue"))))
    )

    sales_by_supplier_country = (
        sales_supplier
        .groupBy("supplier_country")
        .agg(F.sum("sale_total_price").alias("country_revenue"))
    )

    report_supplier_sales = (
        supplier_stats
        .join(sales_by_supplier_country, "supplier_country", "left")
        .select(
            "supplier_id", "supplier_name", "supplier_city", "supplier_country",
            F.round("total_revenue", 2).alias("total_revenue"),
            "total_orders",
            F.round("avg_product_price", 2).alias("avg_product_price"),
            "total_quantity",
            "rank_by_revenue",
            F.round("country_revenue", 2).alias("country_revenue")
        )
        .orderBy("rank_by_revenue")
    )
    write_nodes(df_to_dicts(report_supplier_sales), "ReportSupplierSales")

    print("Creating ReportProductQuality...")
    sales_quality = fact_sales.join(product_full, "product_id", "left")

    quality_stats = (
        sales_quality
        .groupBy("product_id", "product_name", "category_name", "brand_name")
        .agg(
            F.avg("product_rating").alias("avg_rating"),
            F.max("product_rating").alias("max_rating"),
            F.min("product_rating").alias("min_rating"),
            F.sum("product_reviews").alias("total_reviews"),
            F.sum("sale_quantity").alias("total_sales_volume"),
            F.sum("sale_total_price").alias("total_revenue")
        )
        .withColumn("rank_by_rating_desc", F.row_number().over(
            Window.orderBy(F.desc("avg_rating"))))
        .withColumn("rank_by_rating_asc", F.row_number().over(
            Window.orderBy(F.asc("avg_rating"))))
        .withColumn("rank_by_reviews", F.row_number().over(
            Window.orderBy(F.desc("total_reviews"))))
    )

    report_product_quality = (
        quality_stats
        .select(
            "product_id", "product_name", "category_name", "brand_name",
            F.round("avg_rating", 2).alias("avg_rating"),
            "max_rating", "min_rating",
            "total_reviews", "total_sales_volume",
            F.round("total_revenue", 2).alias("total_revenue"),
            "rank_by_rating_desc", "rank_by_rating_asc", "rank_by_reviews"
        )
        .orderBy("rank_by_rating_desc")
    )
    write_nodes(df_to_dicts(report_product_quality), "ReportProductQuality")

    spark.stop()


if __name__ == "__main__":
    main()
