from pyspark.sql import SparkSession
import os
import sys
import logging


def patch_path(*paths):
    return os.path.join(os.path.dirname(__file__), *paths)


def main():
    sys.path.append(patch_path('..'))
    from pyspark_alg.os_utils import set_hadoop_home_dir, is_os_windows
    from pyspark_alg.spark_utils import create_spark_session

    if is_os_windows():
        set_hadoop_home_dir(patch_path('..', 'win-bin'))

    logging.basicConfig(level=logging.WARN)

    spark = create_spark_session('JsonPerson')

    df = spark.read.json(patch_path('data/person.json'))
    data = df.collect()
    print(type(data))
    for person in data:
        print(person)


if __name__ == '__main__':
    main()
