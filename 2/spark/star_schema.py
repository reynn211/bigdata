from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

PG_URL = "jdbc:postgresql://postgres:5432/petstore"
PG_PROPERTIES = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}


def get_spark():
    return (
        SparkSession.builder
        .appName("ETL_CSV_to_StarSchema")

        .getOrCreate()
    )


def write_to_pg(df, table_name, mode="overwrite"):
    df.write.jdbc(url=PG_URL, table=table_name, mode=mode, properties=PG_PROPERTIES)
    print(f"  -> {table_name}: {df.count()} rows written")


def main():
    spark = get_spark()

    raw = spark.read.jdbc(url=PG_URL, table="raw_data", properties=PG_PROPERTIES)
    raw.cache()
    total = raw.count()
    print(f"  Raw data rows: {total}")

    print("\nCreating dim_country...")
    countries = (
        raw.select("customer_country").union(
            raw.select("seller_country")).union(
            raw.select("store_country")).union(
            raw.select("supplier_country"))
        .distinct()
        .withColumnRenamed("customer_country", "country_name")
        .filter(F.col("country_name").isNotNull() & (F.col("country_name") != ""))
    )
    w_country = Window.orderBy("country_name")
    dim_country = countries.withColumn("country_id", F.row_number().over(w_country))
    dim_country = dim_country.select("country_id", "country_name")
    write_to_pg(dim_country, "dim_country")

    country_map = {row.country_name: row.country_id for row in dim_country.collect()}
    country_map_bc = spark.sparkContext.broadcast(country_map)

    @F.udf("int")
    def get_country_id(name):
        if name is None or name == "":
            return None
        return country_map_bc.value.get(name)

    print("Creating dim_brand...")
    brands = (
        raw.select("product_brand")
        .distinct()
        .filter(F.col("product_brand").isNotNull() & (F.col("product_brand") != ""))
        .withColumnRenamed("product_brand", "brand_name")
    )
    w_brand = Window.orderBy("brand_name")
    dim_brand = brands.withColumn("brand_id", F.row_number().over(w_brand))
    dim_brand = dim_brand.select("brand_id", "brand_name")
    write_to_pg(dim_brand, "dim_brand")

    brand_map = {row.brand_name: row.brand_id for row in dim_brand.collect()}
    brand_map_bc = spark.sparkContext.broadcast(brand_map)

    @F.udf("int")
    def get_brand_id(name):
        if name is None or name == "":
            return None
        return brand_map_bc.value.get(name)

    print("Creating dim_category...")
    categories = (
        raw.select("product_category")
        .distinct()
        .filter(F.col("product_category").isNotNull() & (F.col("product_category") != ""))
        .withColumnRenamed("product_category", "category_name")
    )
    w_cat = Window.orderBy("category_name")
    dim_category = categories.withColumn("category_id", F.row_number().over(w_cat))
    dim_category = dim_category.select("category_id", "category_name")
    write_to_pg(dim_category, "dim_category")

    cat_map = {row.category_name: row.category_id for row in dim_category.collect()}
    cat_map_bc = spark.sparkContext.broadcast(cat_map)

    @F.udf("int")
    def get_category_id(name):
        if name is None or name == "":
            return None
        return cat_map_bc.value.get(name)

    print("Creating dim_customer...")
    dim_customer = (
        raw.select(
            F.col("sale_customer_id").alias("customer_id"),
            "customer_first_name",
            "customer_last_name",
            "customer_age",
            "customer_email",
            "customer_country",
            "customer_postal_code",
            "customer_pet_type",
            "customer_pet_name",
            "customer_pet_breed"
        )
        .dropDuplicates(["customer_id"])
        .withColumn("country_id", get_country_id(F.col("customer_country")))
        .drop("customer_country")
    )
    write_to_pg(dim_customer, "dim_customer")

    print("Creating dim_seller...")
    dim_seller = (
        raw.select(
            F.col("sale_seller_id").alias("seller_id"),
            "seller_first_name",
            "seller_last_name",
            "seller_email",
            "seller_country",
            "seller_postal_code"
        )
        .dropDuplicates(["seller_id"])
        .withColumn("country_id", get_country_id(F.col("seller_country")))
        .drop("seller_country")
    )
    write_to_pg(dim_seller, "dim_seller")

    print("Creating dim_store...")
    dim_store = (
        raw.select(
            "store_name",
            "store_location",
            "store_city",
            "store_state",
            "store_country",
            "store_phone",
            "store_email"
        )
        .dropDuplicates(["store_name", "store_city"])
        .withColumn("store_id", F.row_number().over(Window.orderBy("store_name", "store_city")))
        .withColumn("country_id", get_country_id(F.col("store_country")))
        .drop("store_country")
    )
    dim_store = dim_store.select(
        "store_id", "store_name", "store_location", "store_city",
        "store_state", "store_phone", "store_email", "country_id"
    )
    write_to_pg(dim_store, "dim_store")

    store_map = {
        (row.store_name, row.store_city): row.store_id
        for row in dim_store.collect()
    }
    store_map_bc = spark.sparkContext.broadcast(store_map)

    @F.udf("int")
    def get_store_id(name, city):
        if name is None:
            return None
        return store_map_bc.value.get((name, city))

    print("Creating dim_supplier...")
    dim_supplier = (
        raw.select(
            "supplier_name",
            "supplier_contact",
            "supplier_email",
            "supplier_phone",
            "supplier_address",
            "supplier_city",
            "supplier_country"
        )
        .dropDuplicates(["supplier_name", "supplier_city"])
        .withColumn("supplier_id", F.row_number().over(Window.orderBy("supplier_name", "supplier_city")))
        .withColumn("country_id", get_country_id(F.col("supplier_country")))
        .drop("supplier_country")
    )
    dim_supplier = dim_supplier.select(
        "supplier_id", "supplier_name", "supplier_contact", "supplier_email",
        "supplier_phone", "supplier_address", "supplier_city", "country_id"
    )
    write_to_pg(dim_supplier, "dim_supplier")

    supplier_map = {
        (row.supplier_name, row.supplier_city): row.supplier_id
        for row in dim_supplier.collect()
    }
    supplier_map_bc = spark.sparkContext.broadcast(supplier_map)

    @F.udf("int")
    def get_supplier_id(name, city):
        if name is None:
            return None
        return supplier_map_bc.value.get((name, city))

    print("Creating dim_product...")
    dim_product = (
        raw.select(
            F.col("sale_product_id").alias("product_id"),
            "product_name",
            "product_category",
            "product_price",
            "product_quantity",
            "product_weight",
            "product_color",
            "product_size",
            "product_brand",
            "product_material",
            "product_description",
            "product_rating",
            "product_reviews",
            "product_release_date",
            "product_expiry_date",
            "pet_category",
            "supplier_name",
            "supplier_city"
        )
        .dropDuplicates(["product_id"])
        .withColumn("category_id", get_category_id(F.col("product_category")))
        .withColumn("brand_id", get_brand_id(F.col("product_brand")))
        .withColumn("supplier_id", get_supplier_id(F.col("supplier_name"), F.col("supplier_city")))
        .drop("product_category", "product_brand", "supplier_name", "supplier_city")
    )
    write_to_pg(dim_product, "dim_product")

    print("Creating fact_sales...")
    fact_sales = (
        raw.select(
            F.col("id").alias("sale_id"),
            F.col("sale_customer_id").alias("customer_id"),
            F.col("sale_seller_id").alias("seller_id"),
            F.col("sale_product_id").alias("product_id"),
            "store_name",
            "store_city",
            "sale_date",
            "sale_quantity",
            "sale_total_price"
        )
        .withColumn("store_id", get_store_id(F.col("store_name"), F.col("store_city")))
        .withColumn("sale_date_parsed", F.to_date(F.col("sale_date"), "M/d/yyyy"))
        .drop("store_name", "store_city", "sale_date")
        .withColumnRenamed("sale_date_parsed", "sale_date")
    )
    fact_sales = fact_sales.select(
        "sale_id", "customer_id", "seller_id", "product_id",
        "store_id", "sale_date", "sale_quantity", "sale_total_price"
    )
    write_to_pg(fact_sales, "fact_sales")

    spark.stop()


if __name__ == "__main__":
    main()
