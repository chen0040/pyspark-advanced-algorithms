
def left_outer_join(rdd1, rdd2, num_partitions=10):
    rdd1 = rdd1.map(lambda x: (x[0], (x[1], 'L')))
    rdd2 = rdd2.map(lambda x: (x[0], (x[1], 'R')))

    rdd_union = rdd1.union(rdd2).groupByKey(numPartitions=num_partitions)

    def f1(s):
        iterator = s[1]
        left = 'NA'
        right_list = set()
        for data in iterator:
            if data[1] == 'L':
                left = data[0]
            else:
                right_list.add(data[0])
        result = list()
        for right in right_list:
            result.append((left, right))
        return result

    def f2(iterator):
        right_list = set()
        for data in iterator:
            right_list.add(data)
        return right_list

    return rdd_union.flatMap(f1).groupByKey(numPartitions=num_partitions).flatMapValues(f2)
