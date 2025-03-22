from pathlib import Path
import sys

# import pandas as pd # pandas
import pyspark.pandas as pd # # pandas api on spark
from pyspark.sql import SparkSession

root_folder = Path(__file__).parent.parent.parent
sys.path.append(str(root_folder))
from config.config import config as cfg


# Set config and get session spark
builder: SparkSession.Builder = SparkSession.builder.appName("etl-yelp-pandas-json")
builder = builder.config("spark.sql.execution.arrow.pyspark.enabled", "true")
builder.getOrCreate()
print(builder)

# loc json files {local}
json_folder = Path(cfg["folders"]["docs"]["json"])
loc_users = (json_folder / "users.json").as_posix()
loc_business = (json_folder / "business.json").as_posix()
loc_reviews = (json_folder / "reviews.json").as_posix()

# read files
pd_users = pd.read_json(loc_users, lines=True)
pd_business = pd.read_json(loc_business, lines=True)
pd_reviews = pd.read_json(loc_reviews, lines=True)
# <class 'pandas.core.frame.DataFrame'>
# <class 'pyspark.pandas.frame.DataFrame'>

# get df info
pd_users.info()
pd_business.info()
pd_reviews.info()

# describe ds
print(pd_users.describe())

# top results
print(pd_users.head())

# display amount
# print(pd_users.sample(n=10))

# amount of rows
print(len(pd_users.index))

# columns and number of columns
print(pd_users.columns)
print(len(pd_users.columns))

# extract specific range
print(pd_users.loc[0:3])

# pick specific column
print(pd_users.loc[0:1,['name', 'average_stars']])

# unique values
print(pd_business['state'].unique())