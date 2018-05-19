def top_n(spark_context, pair_rdd, n):
    n_broadcast = spark_context.broadcast(n)

    def f1(iterator):
        temp = [(key, value) for (key, value) in iterator]
        temp.sort(key=lambda x: x[1], reverse=True)
        local_n = n_broadcast.value
        print(local_n)
        result = list()
        k = min(local_n, len(temp))
        for item in temp[:k]:
            result.append(item)
        return result

    res = pair_rdd.mapPartitions(f1).collect()
    return res


def bottom_n(spark_context, pair_rdd, n):
    n_broadcast = spark_context.broadcast(n)

    def f1(iterator):
        temp = [(key, value) for (key, value) in iterator]
        temp.sort(key=lambda x: x[1], reverse=False)
        local_n = n_broadcast.value
        print(local_n)
        result = list()
        k = min(local_n, len(temp))
        for item in temp[:k]:
            result.append(item)
        return result

    res = pair_rdd.mapPartitions(f1).collect()
    return res
