"""Structured Streaming job: JSON payment events -> de-duplicated silver table.

- exactly-once file sink + checkpoint (re-running processes only new files)
- stateful de-duplication on event_id bounded by a watermark
- invalid events are filtered here; hourly aggregates are produced in gold by `build_hourly`
  (windowed append-mode aggregations only emit closed windows, which is awkward with
  an availableNow trigger, so the aggregation is a separate batch step).
"""
from pyspark.sql import functions as F, types as T
from .. import config as C
from .spark_session import get_spark, write

SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("order_id", T.LongType()),
    T.StructField("event_ts", T.TimestampType()), T.StructField("method", T.StringType()),
    T.StructField("amount", T.DoubleType()), T.StructField("status", T.StringType()),
])


def run(spark=None, timeout: int = 120) -> int:
    C.ensure_dirs()
    spark = spark or get_spark("stream")
    src = (spark.readStream.schema(SCHEMA).option("maxFilesPerTrigger", 2)
           .json(str(C.LANDING / "stream")))
    clean = (src.where("event_id IS NOT NULL AND amount > 0 AND event_ts IS NOT NULL")
             .withWatermark("event_ts", "7 days")
             .dropDuplicates(["event_id"]))
    q = (clean.writeStream.format("parquet").outputMode("append")
         .option("path", str(C.LAKE / "silver" / "payments"))
         .option("checkpointLocation", str(C.CHECKPOINTS / "payments"))
         .trigger(availableNow=True).start())
    q.awaitTermination(timeout)
    return spark.read.parquet(str(C.LAKE / "silver" / "payments")).count()


def build_hourly(spark=None) -> int:
    spark = spark or get_spark("hourly")
    spark.read.parquet(str(C.LAKE / "silver" / "payments")).createOrReplaceTempView("p")
    df = spark.sql("""
        SELECT date_trunc('hour', event_ts) AS hour_ts, method,
               COUNT(*) AS events,
               SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successes,
               ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN amount ELSE 0 END), 2) AS success_amount
        FROM p GROUP BY 1, 2""")
    write(df, C.LAKE / "gold" / "payments_hourly")
    return df.count()


if __name__ == "__main__":
    print(run(), build_hourly())
