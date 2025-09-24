
# Purpose: Read messy raw data from GCS, clean with PySpark on Dataproc, and write curated Parquet back to GCS.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--raw", required=True, help="GCS path to raw data, e.g., gs://rabbani_proc/raw")
    p.add_argument("--out", required=True, help="GCS path to curated output, e.g., gs://rabbani_processed/curated")
    return p.parse_args()

def normalize_price(col):
    return F.regexp_replace(F.regexp_replace(F.regexp_replace(col, r"[^\d\.\-]", ""), ",", ""), r"\s+", "").cast("double")

def parse_flexible_date(col):
    pats = ["yyyy/MM/dd", "dd-MM-yyyy", "MM-dd-yyyy", "yyyy-MM-dd"]
    return F.coalesce(*[F.to_date(col, p) for p in pats])

def parse_flexible_ts(col):
    pats = ["yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd HH:mm:ssZ", "MM/dd/yyyy HH:mm:ss 'IST'", "yyyy/MM/dd HH:mm:ss'Z'", "yyyy/MM/dd HH:mm:ss"]
    return F.coalesce(*[F.to_timestamp(col, p) for p in pats])

def clean_phone(col):
    return F.regexp_replace(col, r"\D", "")

def valid_email(col):
    return F.regexp_extract(col, r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", 0) != ""

def canonical_category(col):
    return F.when(F.lower(col).isin("accessories","accessory"), F.lit("Accessories")) \
            .when(F.lower(col).isin("mobile phones","mobile","smartphones"), F.lit("Mobiles")) \
            .when(F.lower(col).isin("peripherals","printer","printers"), F.lit("Peripherals")) \
            .when(F.lower(col).isin("laptops","notebooks"), F.lit("Laptops")) \
            .otherwise(F.initcap(col))

def main():
    args = parse_args()
    spark = SparkSession.builder.appName("gcp-de-curation").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    raw_base = args.raw.rstrip("/")
    out_base = args.out.rstrip("/")

    # ---- Customers ----
    customers = spark.read.option("header", True).csv(f"{raw_base}/customers.csv")
    customers_clean = (customers
        .withColumn("customer_id", F.trim(F.col("customer_id")))
        .withColumn("name", F.initcap(F.trim(F.col("name"))))
        .withColumn("email", F.trim(F.col("email")))
        .withColumn("email_is_valid", valid_email(F.col("email")))
        .withColumn("phone_e164ish", clean_phone(F.col("phone")))
        .withColumn("signup_dt", parse_flexible_date(F.col("signup_date")))
        .withColumn("country", F.upper(F.col("country")))
        .withColumn("marketing_opt_in", F.upper(F.coalesce(F.col("marketing_opt_in"), F.lit("N"))))
        .drop("signup_date","phone")
    )
    w = Window.partitionBy("customer_id").orderBy(F.col("signup_dt").desc_nulls_last())
    customers_dedup = (customers_clean
        .withColumn("rn", F.row_number().over(w))
        .filter("rn = 1")
        .drop("rn")
    )

    # ---- Products ----
    products = spark.read.option("header", True).csv(f"{raw_base}/products.csv")
    products_clean = (products
        .withColumn("price_num", normalize_price(F.col("price")))
        .withColumn("cost_num", normalize_price(F.col("cost")))
        .withColumn("margin_pct", F.when(F.col("price_num") > 0, (F.col("price_num")-F.col("cost_num"))/F.col("price_num")).otherwise(F.lit(None)))
        .withColumn("active_flag", F.upper(F.col("active_flag")))
        .withColumn("launch_dt", parse_flexible_date(F.col("launch_date")))
        .withColumn("category_clean", canonical_category(F.col("category")))
        .drop("price","cost","launch_date")
    )

    updates = spark.read.option("header", True).csv(f"{raw_base}/product_price_updates_2025_01.csv") \
        .withColumn("new_price_num", normalize_price(F.col("new_price"))) \
        .withColumn("effective_dt", parse_flexible_date(F.col("effective_date"))) \
        .drop("new_price","effective_date")

    wsku = Window.partitionBy("sku").orderBy(F.col("effective_dt").desc_nulls_last())
    latest_updates = (updates.withColumn("rn", F.row_number().over(wsku)).filter("rn=1").drop("rn"))

    products_final = (products_clean.alias("p")
        .join(latest_updates.alias("u"), on="sku", how="left")
        .withColumn("price_final", F.coalesce(F.col("u.new_price_num"), F.col("p.price_num")))
        .drop("u.new_price_num")
    )

    # ---- Sales ----
    sales = spark.read.option("header", True).csv(f"{raw_base}/sales_2025_01.csv")
    sales_clean = (sales
        .withColumn("quantity_int", F.col("quantity").cast("int"))
        .withColumn("unit_price_num", normalize_price(F.col("unit_price")))
        .withColumn("sale_ts_utc", parse_flexible_ts(F.col("sale_ts")))
        .withColumn("discount_code", F.trim(F.col("discount_code")))
        .withColumn("is_cancelled", F.upper(F.col("status")).isin("CANCELLED","REFUNDED"))
        .filter(~F.col("is_cancelled"))
        .filter(F.col("quantity_int") > 0)
        .withColumn("gross_amount", F.col("quantity_int")*F.col("unit_price_num"))
    )
    sales_clean = (sales_clean
        .withColumn("discount_pct", F.when(F.upper(F.col("discount_code"))=="NEWYEAR10", F.lit(0.10))
                                   .when(F.upper(F.col("discount_code"))=="DISC5", F.lit(0.05))
                                   .otherwise(F.lit(0.0)))
        .withColumn("net_amount", F.col("gross_amount")*(1-F.col("discount_pct")))
    )

    wtx = Window.partitionBy("txn_id","line_id").orderBy(F.col("sale_ts_utc").desc_nulls_last())
    sales_dedup = (sales_clean.withColumn("rn", F.row_number().over(wtx)).filter("rn=1").drop("rn"))

    # ---- Stores ----
    stores = spark.read.option("header", True).csv(f"{raw_base}/store_lookup.csv")
    state_map = F.create_map([F.lit(x) for x in ["TS","Telangana","MH","Maharashtra","CA","California"]])
    stores_clean = stores.withColumn("state_norm", F.coalesce(state_map[F.col("state")], F.col("state"))) \
                         .dropDuplicates(["store_id"])

    # ---- Events ----
    events = spark.read.json(f"{raw_base}/events.jsonl")
    events_exploded = (events
        .withColumn("event", F.explode("events"))
        .select("customer_id",
                F.col("event.type").alias("event_type"),
                F.to_timestamp(F.col("event.ts")).alias("event_ts"),
                F.col("device.os").alias("device_os"),
                F.col("device.app_version").alias("app_version"),
                "ip",
                F.col("event.sku").alias("sku"))
    )

    # ---- Dim tables ----
    dim_customers = customers_dedup.select(
        "customer_id","name","email","email_is_valid","phone_e164ish","signup_dt","country","marketing_opt_in","lat","lon","address"
    )
    dim_products = products_final.select(
        "product_id","sku","name","category_clean","price_final","cost_num","margin_pct","active_flag","launch_dt"
    ).withColumnRenamed("category_clean","category")

    # ---- Fact sales ----
    fact_sales = (sales_dedup.alias("s")
        .join(dim_products.alias("p"), on="product_id", how="left")
        .join(dim_customers.alias("c"), on="customer_id", how="left")
        .join(stores_clean.alias("st"), on="store_id", how="left")
        .select(
            F.col("s.txn_id"),
            F.col("s.line_id"),
            F.col("s.sale_ts_utc"),
            F.col("s.store_id"),
            F.col("st.city"),
            F.col("st.state_norm"),
            F.col("c.customer_id"),
            F.col("c.name").alias("customer_name"),
            F.col("c.email_is_valid"),
            F.col("p.product_id"),
            F.col("p.sku"),
            F.col("p.category"),
            F.col("s.quantity_int"),
            F.col("s.unit_price_num"),
            F.col("s.discount_pct"),
            F.col("s.net_amount")
        )
    )

    # ---- Customer KPIs ----
    wcust = Window.partitionBy("customer_id").orderBy(F.col("sale_ts_utc"))
    customer_kpis = (fact_sales
        .withColumn("order_rank", F.row_number().over(wcust))
        .groupBy("customer_id")
        .agg(
            F.countDistinct("txn_id").alias("orders"),
            F.sum("net_amount").alias("lifetime_value"),
            F.max("sale_ts_utc").alias("last_purchase_ts")
        )
    )

    # ---- Event aggregates ----
    event_counts = (events_exploded
        .groupBy("customer_id","event_type").count()
        .groupBy("customer_id")
        .pivot("event_type", ["view","add_to_cart","purchase"])
        .sum("count")
        .na.fill(0)
    )

    # ---- Customer 360 ----
    customer_360 = (customers_dedup.alias("c")
        .join(customer_kpis.alias("k"), on="customer_id", how="left")
        .join(event_counts.alias("e"), on="customer_id", how="left")
        .select("customer_id","name","signup_dt","country","marketing_opt_in",
                "orders","lifetime_value","last_purchase_ts",
                F.col("view").alias("views"), F.col("add_to_cart").alias("adds"), F.col("purchase").alias("purchases"))
        .na.fill({"orders":0,"lifetime_value":0.0,"views":0,"adds":0,"purchases":0})
    )

    # ---- Write curated outputs ----
    dim_customers.write.mode("overwrite").parquet(f"{out_base}/dim_customers/")
    dim_products.write.mode("overwrite").parquet(f"{out_base}/dim_products/")
    fact_sales.write.mode("overwrite").partitionBy("category").parquet(f"{out_base}/fact_sales/")
    customer_360.write.mode("overwrite").parquet(f"{out_base}/customer_360/")

    spark.stop()

if __name__ == "__main__":
    main()