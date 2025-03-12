from pathlib import Path
import sys

from pyspark.sql import SparkSession

root_folder = Path(__file__).parent.parent.parent
sys.path.append(str(root_folder))
from config.config import config as cfg


# init and instantiate spark session
spark: SparkSession = SparkSession.builder.getOrCreate()

# get configured folders and files
device_folder = Path(cfg["folders"]["docs"]["device"])
subscription_folder = Path(cfg["folders"]["docs"]["subscription"])

device_json_files = (device_folder / "*.json").as_posix()
subscription_json_files = (subscription_folder / "*.json").as_posix()

# data import
spark.sql(f"""
    CREATE TEMPORARY VIEW vw_device
    USING org.apache.spark.sql.json
    OPTIONS (path "{device_json_files}")
""")

spark.sql(f"""
    CREATE TEMPORARY VIEW vw_subscription
    USING org.apache.spark.sql.json
    OPTIONS (path "{subscription_json_files}")
""")

print(spark.catalog.listTables())

# select data
spark.sql("""SELECT * FROM vw_device LIMIT 10""").show()
spark.sql("""SELECT * FROM vw_subscription LIMIT 10""").show()

# new dataframe = {join}
join_datasets = spark.sql("""
    SELECT dev.user_id,
           dev.model,
           dev.platform,
           subs.payment_method,
           subs.plan
    FROM vw_device AS dev
    INNER JOIN vw_subscription AS subs
    ON dev.user_id = subs.user_id
""")

# info
join_datasets.show()
join_datasets.printSchema()
join_datasets.count()