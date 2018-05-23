import sys
import os


def patch_path(*paths):
    return os.path.join(os.path.dirname(__file__), *paths)


def main():
    sys.path.append(patch_path('..'))
    from pyspark_alg.os_utils import set_hadoop_home_dir, is_os_windows
    from pyspark_alg.spark_utils import create_local_spark_context
    from pyspark_alg.joining import left_outer_join

    if is_os_windows():
        set_hadoop_home_dir(patch_path('..', 'win-bin'))

    spark = create_local_spark_context('topN')

    rdd1 = spark.parallelize(
        [('Chair', 1000), ('Table', 3000), ('Chair2', 2000), ('Table2', 6000), ('Bed', 2000), ('Bed2', 4000)])
    rdd2 = spark.parallelize([('Chair', 'British'), ('Table', 'English'), ('Bed', 'USA')])

    result_rdd = left_outer_join(rdd1, rdd2, num_partitions=4)
    res = result_rdd.collect()

    for data in res:
        print(data)


if __name__ == '__main__':
    main()
