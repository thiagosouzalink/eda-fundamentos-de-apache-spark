from pyspark.sql import DataFrame


def write_into_parquet(df: DataFrame, 
                       mode: str, 
                       location: str) -> None:
    """Write the DataFrame into a Parquet file.

    Args:
        df (DataFrame): The input Spark DataFrame.
        mode (str): The write mode.
        location (str): The location (path) where the Parquet file will 
                        be saved.
    """

    df.write.format("parquet").mode(mode).save(location)
    return None