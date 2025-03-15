from pyspark.sql import DataFrame
from pyspark.sql.functions import *


def get_greatest_rank(df: DataFrame) -> DataFrame:
    """Calculate the greatest rank among specified columns.

    Args:
        df (DataFrame): The input Spark DataFrame.

     Returns:
        DataFrame: A new Spark DataFrame with an additional column 
                  'highest_rate' representing the greatest value across 
                  the specified columns for each row.
    """

    df_transformed = df.select(
        greatest("compliment_cool", 
                 "compliment_cute", 
                 "compliment_funny", 
                 "compliment_hot").alias("highest_rate")
    )

    return df_transformed


def get_rate(df: DataFrame) -> DataFrame:
    """Assign a rank based on the 'useful' column.

    Args:
        df (DataFrame): The input Spark DataFrame.

    Returns:
        DataFrame: A new Spark DataFrame transformed.
    """
    df_transformed = df.withColumn(
        "rank",
        when(col("useful") >= 500, lit("high")).otherwise(lit("low"))
    )

    return df_transformed