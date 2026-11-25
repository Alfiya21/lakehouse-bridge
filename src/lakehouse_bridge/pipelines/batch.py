"""Batch medallion pipeline in PySpark: landing CSV -> bronze -> silver (+quarantine) -> gold.

Design notes
- bronze keeps raw strings plus lineage columns (_ingest_ts, _source_file).
- silver enforces types, de-duplicates on the business key (latest ingest wins) and
  routes rejected rows to a quarantine table with a machine-readable reason.
- gold is built with Spark SQL (window functions, NTILE, DENSE_RANK).
"""
import json
from pyspark.sql import functions as F, Window
from .. import config as C
from .spark_session import get_spark, write


def run(spark=None) -> dict:
    C.ensure_dirs()
    spark = spark or get_spark("batch")
    batch = C.LANDING / "batch"
    m = {}

    # ---------------- bronze ----------------
    def bronze(name):
        df = (spark.read.option("header", True).csv(str(batch / f"{name}.csv"))
              .withColumn("_ingest_ts", F.current_timestamp())
              .withColumn("_source_file", F.input_file_name()))
        write(df, C.LAKE / "bronze" / name)
        return df

    b_c, b_o = bronze("customers"), bronze("orders")
    m["bronze_customers"], m["bronze_orders"] = b_c.count(), b_o.count()

    # ---------------- silver: customers ----------------
    cust = (b_c.select(
        F.col("customer_id").cast("long"), F.trim("full_name").alias("full_name"),
        F.lower(F.trim("email")).alias("email"), F.trim("phone").alias("phone"),
        F.upper("region").alias("region"), F.upper("segment").alias("segment"),
        F.to_date("signup_date").alias("signup_date"), "_ingest_ts")
        .where(F.col("customer_id").isNotNull()))
    w = Window.partitionBy("customer_id").orderBy(F.col("_ingest_ts").desc())
    cust = cust.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn", "_ingest_ts")
    write(cust, C.LAKE / "silver" / "customers")
    m["silver_customers"] = cust.count()

    # ---------------- silver: orders (+ quarantine) ----------------
    o = b_o.select(
        F.col("order_id").cast("long"), F.col("customer_id").cast("long"),
        F.to_timestamp("order_ts").alias("order_ts"), F.col("amount").cast("double"),
        F.upper("status").alias("status"), "currency", "_ingest_ts")
    w = Window.partitionBy("order_id").orderBy(F.col("_ingest_ts").desc())
    o = o.withColumn("_rn", F.row_number().over(w))
    dup_count = o.where("_rn > 1").count()
    o = o.where("_rn = 1").drop("_rn")

    o = o.join(cust.select("customer_id", "region"), "customer_id", "left")
    reason = (F.when(F.col("order_id").isNull(), "NULL_ORDER_ID")
              .when(F.col("amount").isNull() | (F.col("amount") <= 0), "NON_POSITIVE_AMOUNT")
              .when(F.col("region").isNull(), "ORPHAN_CUSTOMER"))
    o = o.withColumn("_reason", reason)
    quarantine = o.where("_reason IS NOT NULL").drop("_ingest_ts").withColumnRenamed("_reason", "reason")
    good = (o.where("_reason IS NULL").drop("_reason", "_ingest_ts")
            .withColumn("order_date", F.to_date("order_ts"))
            .withColumn("order_month", F.date_format("order_ts", "yyyy-MM")))
    write(good, C.LAKE / "silver" / "orders", partition_by="order_month")
    write(quarantine, C.LAKE / "quarantine" / "orders")
    m.update(silver_orders=good.count(), quarantine_orders=quarantine.count(), duplicates_removed=dup_count)

    # ---------------- gold (Spark SQL) ----------------
    silver_o = spark.read.format(C.FORMAT).load(str(C.LAKE / "silver" / "orders"))
    silver_o.createOrReplaceTempView("silver_orders")
    cust.createOrReplaceTempView("silver_customers")

    daily = spark.sql("""
        SELECT order_date, region,
               COUNT(*)                        AS orders,
               ROUND(SUM(amount), 2)           AS revenue,
               ROUND(AVG(amount), 2)           AS avg_order_value
        FROM silver_orders
        WHERE status <> 'CANCELLED'
        GROUP BY order_date, region""")
    write(daily, C.LAKE / "gold" / "daily_revenue")

    ltv = spark.sql("""
        WITH agg AS (
          SELECT c.customer_id, c.region, c.segment,
                 COUNT(o.order_id)              AS order_count,
                 ROUND(SUM(o.amount), 2)        AS total_revenue,
                 MIN(o.order_date)              AS first_order_date,
                 MAX(o.order_date)              AS last_order_date
          FROM silver_customers c
          JOIN silver_orders o ON o.customer_id = c.customer_id AND o.status <> 'CANCELLED'
          GROUP BY c.customer_id, c.region, c.segment)
        SELECT *,
               DENSE_RANK() OVER (PARTITION BY region ORDER BY total_revenue DESC) AS revenue_rank_in_region,
               NTILE(4)     OVER (ORDER BY total_revenue DESC)                     AS ltv_quartile
        FROM agg""")
    write(ltv, C.LAKE / "gold" / "customer_ltv")
    m["gold_daily_revenue"], m["gold_customer_ltv"] = daily.count(), ltv.count()

    (C.REPORTS / "batch_metrics.json").write_text(json.dumps(m, indent=2))
    return m


if __name__ == "__main__":
    print(run())
