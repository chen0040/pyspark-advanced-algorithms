import sys
import os

def patch_path(*paths):
    return os.path.join(os.path.dirname(__file__), *paths)


def main():
    sys.path.append(patch_path('..'))
    from pyspark_alg.os_utils import set_hadoop_home_dir, is_os_windows
    from pyspark_alg.spark_utils import create_local_spark_context
    from pyspark_alg.filtering import bottom_n

    if is_os_windows():
        set_hadoop_home_dir(patch_path('..', 'win-bin'))

    spark = create_local_spark_context('topN')

    pair_rdd = spark.parallelize([('Chair', 1000), ('Table', 3000), ('Chair2', 2000), ('Table2', 6000), ('Bed', 2000), ('Bed2', 4000)])

    res = bottom_n(spark, pair_rdd, n=3)

    for data in res:
        print(data)

    print('Session created')


if __name__ == '__main__':
    main()
