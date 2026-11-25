from pyspark.sql import SparkSession
from .. import config as C


def get_spark(app: str = "lakehouse-bridge") -> SparkSession:
    b = (SparkSession.builder.appName(app).master("local[2]")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.ui.enabled", "false")
         .config("spark.driver.host", "127.0.0.1")
         .config("spark.sql.session.timeZone", "UTC"))
    if C.FORMAT == "delta":   # needs Delta jars (Databricks has them; on Dataproc pass --packages io.delta:delta-spark_2.12:3.1.0)
        b = (b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"))
    return b.getOrCreate()


def write(df, path, partition_by=None, mode="overwrite"):
    w = df.write.mode(mode).format(C.FORMAT)
    if partition_by:
        w = w.partitionBy(partition_by)
    w.save(str(path))
