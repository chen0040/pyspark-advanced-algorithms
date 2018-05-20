import sys
import os


def patch_path(*paths):
    return os.path.join(os.path.dirname(__file__), *paths)


def main():
    sys.path.append(patch_path('..'))
    from pyspark_alg.os_utils import set_hadoop_home_dir, is_os_windows
    from pyspark_alg.spark_utils import create_local_spark_context
    from pyspark_alg.joining import find_common_friends

    if is_os_windows():
        set_hadoop_home_dir(patch_path('..', 'win-bin'))

    spark = create_local_spark_context('topN')

    adj_v = dict()
    adj_v['jim'] = ['james', 'robert', 'jack', 'jackie', 'alice']
    adj_v['james'] = ['jim', 'robert', 'alice']
    adj_v['robert'] = ['james', 'jim', 'alice']
    adj_v['jackie'] = ['jim', 'jack']
    adj_v['jack'] = ['jackie', 'jim']

    rdd = spark.parallelize([(person, friendList) for person, friendList in adj_v.items()])

    result_rdd = find_common_friends(rdd)

    res = result_rdd.collect()

    for data in res:
        print(data)


if __name__ == '__main__':
    main()
