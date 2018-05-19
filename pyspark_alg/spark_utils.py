from pyspark.sql import SparkSession


def create_spark_context(name):
    return SparkSession \
        .builder \
        .appName(name) \
        .getOrCreate()
