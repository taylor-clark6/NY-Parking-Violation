from __future__ import print_function
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_timestamp

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: violationDate <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession \
        .builder \
        .appName("ViolationDateCount") \
        .getOrCreate()

    # Read CSV with headers
    csv_path = sys.argv[1]
    df = spark.read.option("header", True).csv(csv_path)

    # Parse issue_date into DateType (assumes format is 'MM/dd/yyyy' or similar)
    df = df.withColumn("issue_date_parsed", to_timestamp(col("issue_date"), "yyyy-MM-dd'T'HH:mm:ss.SSS"))

    # Extract just month and day as 'MM-dd'
    df = df.withColumn("month_day", date_format(col("issue_date_parsed"), "MM-dd"))

    # Group by month_day and count
    date_counts = df.groupBy("month_day").count().orderBy("count", ascending=False)

    # Collect & print
    for row in date_counts.collect():
        print('"{}": {}'.format(row["month_day"], row["count"]))

    # Get the date with the highest count (first row of sorted results)
    top_row = date_counts.first()
    if top_row:
        print('\nTickets are most likely to be issued on: "{}" with {} violations having been issued on this date.'.format(top_row["month_day"], top_row["count"]))

    spark.stop()

