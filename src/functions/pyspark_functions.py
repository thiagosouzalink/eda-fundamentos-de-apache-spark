from pathlib import Path

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import upper

root_folder = Path(__file__).parent.parent.parent
sys.path.append(str(root_folder))
from config.config import config as cfg


spark: SparkSession = SparkSession \
                      .builder \
                      .appName("py-functions") \
                      .getOrCreate()
                              
print(SparkConf().getAll())
spark.sparkContext.setLogLevel("ERROR")

# get configured folders and files
json_folder = Path(cfg["folders"]["docs"]["json"])
loc_users = (json_folder / "users.json").as_posix()
loc_business = (json_folder / "business.json").as_posix()

df_users = spark.read.json(loc_users)
df_business = spark.read.json(loc_business)

df_users.printSchema()
print(df_users.show())

#---------------------------------------------#
#                   functions
#---------------------------------------------#

# select
df_users.select("user_id", 
                "name", 
                "average_stars", 
                "review_count", 
                "fans", 
                "yelping_since").show()
df_users.select(col("user_id"), col("name")).show()

# lit = add literal or constant
# withColumn = add new column to df or manipulate
# when
df_users.select(lit(1).alias("valid_row")).show()
df_users.withColumn(
    "rank", 
    when(col("useful") >= 500, lit("high")).otherwise(lit("low"))
).show()

# monotonically_increasing_id = generated id
df_users.select(monotonically_increasing_id().alias("event"), "name").show(2)

# greatest = greatest value of the list
df_users.select(
    "name",
    "compliment_cool",
    "compliment_cute",
    "compliment_funny",
    "compliment_hot",
    greatest("compliment_cool", 
             "compliment_cute", 
             "compliment_funny", 
             "compliment_hot").alias("highest_rate")
).show()

# expr = expression string into the column that it represents {sql-like expressions}
df_users.select(
    expr("CASE WHEN useful >= 500 THEN 'high' " + " ELSE 'low' END").alias("score")
).show()

# round = given value to scale decimal places
df_users.select(col("average_stars"), round("average_stars", 0)).show()

# current_date & current timestamp = current date & timestamp
df_users.select(current_date(), current_timestamp()).show()

# year = retrieve year
df_users.select(year(current_timestamp())).alias("year").show()

# pyspark.sql.DataFrame.transform() {3.0} & pyspark.sql.functions.transform() = applying a transformation to each element
def verify_rank(df):
    return df_users.withColumn(
        "rank", 
        when(col("useful") >= 500, lit("high")).otherwise(lit("low"))
    )
    

# avg = returns the average of the values in a group
df_users.select(avg("average_stars")).show()