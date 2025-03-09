from pathlib import Path
import platform
import sys

from pyspark.sql import SparkSession


root_folder = Path(__file__).parent.parent.parent
sys.path.append(str(root_folder))
from config.config import config as cfg


# Init spark session
spark: SparkSession = SparkSession.builder.getOrCreate()

# load data
device_folder = Path(cfg["folders"]["docs"]["device"])

json_file = (device_folder / "device_2022_6_7_19_39_24.json").as_posix()
df_device = spark.read.json(json_file)

json_files = (device_folder / "*.json").as_posix()
df_device = spark.read.json(json_files)

# schema
df_device.printSchema()

# columns
print(df_device.columns)

# rows
print(df_device.count())

# show df
df_device.show()

# select columns
df_device.select("manufacturer", "model", "platform").show()
df_device.selectExpr("manufacturer", "model", "platform as type").show()

# filter
df_device.filter(df_device.manufacturer == "Xiamomi").show()

# group by
df_device.groupBy("manufacturer").count().show()

df_grouped_manufacturer = df_device.groupBy("manufacturer").count()
df_grouped_manufacturer.show()
