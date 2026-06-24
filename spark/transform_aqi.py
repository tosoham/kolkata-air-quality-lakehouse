"""Bronze -> Silver transform for Kolkata WBPCB hourly AQI files.

The raw CSVs are a nested matrix per station:

    Year,2017
    January-2017,00:00:00,01:00:00, ... ,23:00:00     <- month header (hour labels)
    1,30.0,26.0, ...                                  <- day row: day number + 24 hourly AQI values
    2,31.0, ...
    ...
    Year,2018
    ...

This job reads each raw file whole (so the irregular structure can be parsed in
Python), unpivots it into one row per (station, timestamp, aqi), and writes
partitioned Parquet to the silver bucket plus a daily-average aggregate.
"""

import argparse
import csv
import io
import os
import sys
from datetime import datetime

from pyspark.sql import SparkSession, functions as F, types as T

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11,
    "december": 12,
}


def build_spark_session(minio_endpoint, access_key, secret_key, app_name="aqi_transform"):
    """Build a SparkSession configured to talk to MinIO via s3a."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()

    hconf = spark.sparkContext._jsc.hadoopConfiguration()
    if minio_endpoint:
        hconf.set("fs.s3a.endpoint", minio_endpoint)
    if access_key:
        hconf.set("fs.s3a.access.key", access_key)
    if secret_key:
        hconf.set("fs.s3a.secret.key", secret_key)
    hconf.set("fs.s3a.connection.ssl.enabled", "false")
    hconf.set("fs.s3a.path.style.access", "true")
    hconf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    return spark


def station_from_path(path):
    """aqi_ballygunge_kolkata_wbpcb_hourly.csv -> 'ballygunge'."""
    name = os.path.splitext(os.path.basename(path))[0]
    if name.startswith("aqi_"):
        name = name[len("aqi_"):]
    return name.split("_kolkata")[0] or name


def parse_file(path, content):
    """Yield (station, timestamp, aqi) tuples from one raw AQI file's content."""
    # Only the aqi_* station files use this matrix layout.
    if "aqi_" not in os.path.basename(path).lower():
        return

    station = station_from_path(path)
    cur_year = None
    cur_month = None

    for row in csv.reader(io.StringIO(content)):
        if not row:
            continue
        head = row[0].strip()
        if not head:
            continue

        # "Year,2017"
        if head.lower() == "year" and len(row) >= 2 and row[1].strip().isdigit():
            cur_year = int(row[1].strip())
            continue

        # "January-2017,00:00:00,..." month header (also carries the year)
        if "-" in head:
            month_part = head.split("-", 1)[0].strip().lower()
            year_part = head.split("-", 1)[1].strip()
            if month_part in MONTHS:
                cur_month = MONTHS[month_part]
                if year_part.isdigit():
                    cur_year = int(year_part)
                continue

        # Day row: "1,30.0,26.0,..." (24 hourly values)
        if head.isdigit():
            if cur_year is None or cur_month is None:
                continue
            day = int(head)
            for hour in range(24):
                idx = hour + 1
                if idx >= len(row):
                    break
                val = row[idx].strip()
                if val in ("", "NA", "N/A", "None", "-", "nan", "NaN"):
                    continue
                try:
                    aqi = float(val)
                except ValueError:
                    continue
                try:
                    ts = datetime(cur_year, cur_month, day, hour)
                except ValueError:
                    continue  # e.g. day 31 in a 30-day month
                yield (station, ts, aqi)


def main(args):
    spark = build_spark_session(args.minio_endpoint, args.minio_access, args.minio_secret)
    sc = spark.sparkContext

    bronze_path = f"s3a://{args.bronze_bucket}/{args.bronze_prefix}".rstrip("/") + "/*.csv"
    print("Reading raw CSVs from", bronze_path)

    raw = sc.wholeTextFiles(bronze_path)
    records = raw.flatMap(lambda kv: list(parse_file(kv[0], kv[1])))

    schema = T.StructType([
        T.StructField("station", T.StringType(), False),
        T.StructField("timestamp", T.TimestampType(), False),
        T.StructField("aqi", T.DoubleType(), False),
    ])

    df = spark.createDataFrame(records, schema=schema)

    if df.rdd.isEmpty():
        print("No AQI records parsed from bronze, exiting.")
        spark.stop()
        sys.exit(0)

    df = (
        df.withColumn("date", F.to_date("timestamp"))
        .withColumn("year", F.year("timestamp"))
        .withColumn("month", F.month("timestamp"))
        .withColumn("day", F.dayofmonth("timestamp"))
        .dropDuplicates(["station", "timestamp"])
    )

    daily_avg = df.groupBy("station", "date", "year", "month", "day").agg(
        F.round(F.avg("aqi"), 2).alias("avg_aqi"),
        F.min("aqi").alias("min_aqi"),
        F.max("aqi").alias("max_aqi"),
        F.count("*").alias("obs_count"),
    )

    out_base = f"s3a://{args.silver_bucket}/aqi"

    print("Writing cleaned records to", f"{out_base}/records")
    (
        df.write.mode("overwrite")
        .partitionBy("year", "month", "station")
        .parquet(f"{out_base}/records", compression="snappy")
    )

    print("Writing daily aggregates to", f"{out_base}/daily")
    (
        daily_avg.write.mode("overwrite")
        .partitionBy("year", "month")
        .parquet(f"{out_base}/daily", compression="snappy")
    )

    total = df.count()
    print(f"Done. Wrote {total} hourly records across {df.select('station').distinct().count()} stations.")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze-bucket", required=True)
    parser.add_argument("--bronze-prefix", default="")
    parser.add_argument("--silver-bucket", required=True)
    parser.add_argument("--minio-endpoint", default="http://minio:9000")
    parser.add_argument("--minio-access", default=None)
    parser.add_argument("--minio-secret", default=None)
    args = parser.parse_args()
    main(args)
