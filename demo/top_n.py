from pyspark.sql import SparkSession


def main():
    spark = SparkSession \
            .builder \
            .appName('TopN') \
            .getOrCreate()

    print('Session created')


if __name__ == '__main__':
    main()
