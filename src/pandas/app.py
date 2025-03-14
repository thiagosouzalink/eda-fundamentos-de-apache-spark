""""
*****************************
Pandas API on Spark {PyArrow}
* PySpark
* Koalas
* PySpark.Pandas

*****************************
Not 100% Compatible Yet {WIP}
httpd://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/index.html

*****************************
Recommended Path
Already Spark Developer = PySpark’s API
Pandas Developer = Pandas API on Spark {Does Not Support Structured Streaming Officially}

*****************************

Spark DataFrame is Converted into Pandas-on-Spark DataFrame
sequence
distributed-sequence
distributed
httpd://spark.apache.org/docs/latest/api/python/user_guide/pandas_on_spark/options.html#default-index-type
"""
from pathlib import Path
import sys

import pyspark.pandas as ps
from pyspark.sql import SparkSession

root_folder = Path(__file__).parent.parent.parent
sys.path.append(str(root_folder))
from config.config import config as cfg


# set configs
builder: SparkSession.Builder = SparkSession.builder.appName("app")
builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
builder.getOrCreate()
print(builder)

# get configured folders and files
device_folder = Path(cfg["folders"]["docs"]["device"])
subscription_folder = Path(cfg["folders"]["docs"]["subscription"])

device_json_files = (device_folder / "*.json").as_posix()
subscription_json_files = (subscription_folder / "*.json").as_posix()

# pandas on spark - read files
get_device = ps.read_json(device_json_files)
get_subscription = ps.read_json(subscription_json_files)

# # print data
# print(get_device)
# print(get_subscription)

# # get info
# get_device.info()
# get_subscription.info()

# get plan
get_device.spark.explain(mode="formatted")
get_subscription.spark.explain(mode="formatted")