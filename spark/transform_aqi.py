import argparse
import sys
from pyspark.sql import SparkSession, functions as F, types as T


def build_spark_session(minio_endpoint, access_key, secret_key, app_name="aqi_transform"):
    """
    Build a SparkSession configured to talk to MinIO via s3a.
    Access/secret/endpoint typically pulled from Airflow connection or env vars.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        # Add any other spark configs you need here
    )
    spark = builder.getOrCreate()

    # Configure Hadoop S3A for MinIO
    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    hconf.set("fs.s3a.endpoint", minio_endpoint)
    hconf.set("fs.s3a.access.key", access_key)
    hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.connection.ssl.enabled", "false")  # change if using TLS
    hconf.set("fs.s3a.path.style.access", "true")
    # Recommended tuning
    hconf.set("fs.s3a.connection.maximum", "100")
    hconf.set("fs.s3a.multipart.size", "104857600")  # 100 MB
    return spark


def infer_timestamp_col(df):
    # Look for commonly used timestamp column names
    possible = ["timestamp", "time", "datetime", "date", "created_at"]
    cols = [c.lower() for c in df.columns]
    for p in possible:
        if p in cols:
            return df.columns[cols.index(p)]
    # fallback: try to find a column containing 'time' or 'date'
    for i, c in enumerate(cols):
        if "time" in c or "date" in c:
            return df.columns[i]
    return None


def main(args):
    # get minio creds from env (Airflow provides via connection or mounted secrets)
    MINIO_ENDPOINT = args.minio_endpoint or "http://minio:9000"
    MINIO_ACCESS = args.minio_access or ""
    MINIO_SECRET = args.minio_secret or ""

    spark = build_spark_session(MINIO_ENDPOINT, MINIO_ACCESS, MINIO_SECRET)

    bronze_path = f"s3a://{args.bronze_bucket}/{args.bronze_prefix}".rstrip("/") + "/*"
    print("Reading CSVs from", bronze_path)

    # read CSVs (allow header and schema inference)
    df = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(bronze_path)
    )

    if df.rdd.isEmpty():
        print("No data found in bronze path, exiting.")
        spark.stop()
        sys.exit(0)

    # detect timestamp column
    ts_col = infer_timestamp_col(df)
    if ts_col is None:
        raise RuntimeError("No timestamp-like column found in bronze CSVs")

    # normalize and cast
    df = df.withColumn("raw_ts", F.col(ts_col))
    # attempt several timestamp formats
    df = df.withColumn(
        "timestamp",
        F.coalesce(
            F.to_timestamp("raw_ts", "yyyy-MM-dd HH:mm:ss"),
            F.to_timestamp("raw_ts", "yyyy/MM/dd HH:mm:ss"),
            F.to_timestamp("raw_ts", "dd-MM-yyyy HH:mm:ss"),
            F.to_timestamp("raw_ts")
        ),
    )

    # fallback: if timestamp could not be parsed, try date-only
    df = df.withColumn("timestamp", F.coalesce("timestamp", F.to_timestamp("raw_ts", "yyyy-MM-dd")))

    # extract date parts
    df = df.withColumn("date", F.to_date("timestamp"))
    df = df.withColumn("year", F.year("timestamp"))
    df = df.withColumn("month", F.month("timestamp"))
    df = df.withColumn("day", F.dayofmonth("timestamp"))

    # standardize station column name: try to find station-like column
    station_col = None
    for c in df.columns:
        if "station" in c.lower() or "site" in c.lower() or "location" in c.lower():
            station_col = c
            break
    if station_col is None:
        # fallback: use filename (if available) or set unknown
        df = df.withColumn("station", F.lit("unknown"))
    else:
        df = df.withColumnRenamed(station_col, "station")

    # cast numeric columns (attempt)
    for c in df.columns:
        # naive heuristic: if column name contains 'aqi' or 'pm'
        if "aqi" in c.lower() or "pm" in c.lower():
            df = df.withColumn(c, F.col(c).cast(T.DoubleType()))

    # drop exact duplicates
    df = df.dropDuplicates()

    # compute daily aggregates as an example
    daily_avg = (
        df.groupBy("station", "date", "year", "month", "day")
        .agg(
            F.avg("aqi").alias("avg_aqi"),
            F.count("*").alias("obs_count"),
        )
    )

    # write cleaned records to silver (partitioned)
    out_base = f"s3a://{args.silver_bucket}/aqi"
    print("Writing cleaned Parquet to", out_base)
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month", "day", "station")
        .parquet(f"{out_base}/records", compression="snappy")
    )

    # write daily aggregates too
    (
        daily_avg.write.mode("overwrite")
        .partitionBy("year", "month", "day")
        .parquet(f"{out_base}/daily", compression="snappy")
    )

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-bucket", required=True)
    parser.add_argument("--bronze-prefix", default="")
    parser.add_argument("--silver-bucket", required=True)
    # MinIO creds/endpoint can be provided via environment variables in container or via Airflow secrets
    parser.add_argument("--minio-endpoint", default=None)
    parser.add_argument("--minio-access", default=None)
    parser.add_argument("--minio-secret", default=None)
    args = parser.parse_args()
    main(args)
