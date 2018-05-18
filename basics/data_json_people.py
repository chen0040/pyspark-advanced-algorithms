from pyspark.sql import SparkSession

def main():
    spark = SparkSession \
        .builder \
        .appName('JsonPeople') \
        .getOrCreate()
    df = spark.read.format('json').load('py/test/sql/people.json')