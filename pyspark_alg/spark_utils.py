from pyspark.sql import SparkSession
from pyspark.context import SparkContext


def create_spark_session(name):
    return SparkSession \
        .builder \
        .appName(name) \
        .getOrCreate()


def create_local_spark_context(name):
    return SparkContext('local', name)


