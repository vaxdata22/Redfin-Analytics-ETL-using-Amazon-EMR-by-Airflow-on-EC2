
# Import the necessary PySpark libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, date_format, regexp_replace

# Start Spark session
spark = SparkSession.builder.appName("RedfinDataCleaning").getOrCreate()

# The function that would carry out the transformation of the Redfin data
def transform_redfin_data():
    # Define the appropriate S3 paths
    raw_data_s3_bucket = "s3://redfin-analytics-emr-etl-bucket/raw-data/city_market_tracker.tsv000.gz"
    transform_data_s3_bucket = "s3://redfin-analytics-emr-etl-bucket/transformed-data/"

    # Read data (gzipped TSV file)
    df = spark.read.csv(raw_data_s3_bucket, header=True, inferSchema=True, sep="\t")

    # Select the 24 desired columns
    selected_cols = [
        'period_begin', 'period_end', 'period_duration', 'region_type', 'region_type_id', 'table_id',
        'is_seasonally_adjusted', 'city', 'state', 'state_code', 'property_type', 'property_type_id',
        'median_sale_price', 'median_list_price', 'median_ppsf', 'median_list_ppsf', 'homes_sold',
        'inventory', 'months_of_supply', 'median_dom', 'avg_sale_to_list', 'sold_above_list',
        'parent_metro_region_metro_code', 'last_updated'
    ] 
    df = df.select(selected_cols)

    # Remove commas from 'city' field
    df = df.withColumn("city", regexp_replace(col("city"), ",", ""))

    # Drop records with any nulls
    df = df.dropna()

    # Convert 'period_begin' and 'period_end' to proper dates
    df = df.withColumn("period_begin", col("period_begin").cast("date"))
    df = df.withColumn("period_end", col("period_end").cast("date"))

    # Extract year from period_begin and period_end into new columns
    df = df.withColumn("period_begin_in_years", year(col("period_begin")))
    df = df.withColumn("period_end_in_years", year(col("period_end")))

    # Extract month name from period_begin and period_end into new columns
    df = df.withColumn("period_begin_in_months", date_format(col("period_begin"), "MMMM"))
    df = df.withColumn("period_end_in_months", date_format(col("period_end"), "MMMM"))

    # Write to S3 as Parquet file
    df.write.mode("overwrite").parquet(transform_data_s3_bucket)

# Run the transformation
transform_redfin_data()
