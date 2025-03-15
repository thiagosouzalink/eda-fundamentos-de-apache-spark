from pathlib import Path
import sys

from pyspark.sql import SparkSession
from pyspark import SparkConf

root_folder = Path(__file__).parent.parent.parent
sys.path.append(str(root_folder))
from utils.input import read_parquet_files
from utils.transforms import get_greatest_rank, get_rate
from utils.output import write_into_parquet
from config.config import config as cfg


# main definitions & calls
def main():
    spark: SparkSession = SparkSession \
                          .builder \
                          .appName("etl-job-users") \
                          .getOrCreate()
                          
    # configs
    print(spark)
    print(SparkConf().getAll())
    spark.sparkContext.setLogLevel("INFO")
    
    # extract {E}
    parquet_folder = Path(cfg["folders"]["docs"]["parquet"])
    output_folder = Path(cfg["folders"]["docs"]["output"])
    users_filepath = (parquet_folder / "users").as_posix()
    file_output = (output_folder / "users").as_posix()
    df_users = read_parquet_files(spark=spark, filename=users_filepath)
    df_users.show()
    
    # transform {E}
    df_rank = get_greatest_rank(df=df_users)
    df_rank.show()
    df_users_transformed = get_rate(df=df_users)
    
    # load {L}
    write_into_parquet(df=df_users_transformed, 
                       mode="overwrite", 
                       location=file_output)
    
    
# entry point for pyspark etl app
if __name__ == '__main__':
    main()