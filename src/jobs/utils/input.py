from pyspark.sql import SparkSession, DataFrame


def read_parquet_files(spark: SparkSession, 
                       filename: str) -> DataFrame:
    """Load data from json file format.

    Args:
        spark (SparkSession): Spark session object.
        filename (str): Location of the file.

    Returns:
        DataFrame: Spark dataFrame.
    """

    df = spark.read.parquet(filename)
    return df