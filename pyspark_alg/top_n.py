from pyspark.sql import SparkSession

spark = SparkSession \
        .builder \
        .appName('CalculatingGeoDistances') \
        .getOrCreate()

print('Session created')