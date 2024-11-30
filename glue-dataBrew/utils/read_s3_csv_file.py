from pyspark.sql import SparkSession

def read_s3_csv_file(bucket_name: str,
                     file_path: str):

    """
    Reads a CSV file

    Args:
    - bucket_name
    - file_path (str)

    Returns:
    - df (pyspark.sql.DataFrame)
    """
    ## Creates a SparkSession
    spark = SparkSession.builder.appName("Read S3 CSV File").getOrCreate()

    ## Reading the CSV file from S3
    df = spark.read.format("csv") \
        .option("header","true") \
        .option("inferSchema","true") \
        .load(f"s3://{bucket_name}/{file_path}")

    ## Returns a Spark DataFrame
    return df
