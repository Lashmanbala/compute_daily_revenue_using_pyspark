from pyspark.sql import SparkSession

spark = SparkSession. \
            builder. \
            appName('compute_daily_revenue'). \
            master('yarn'). \
            getOrCreate()