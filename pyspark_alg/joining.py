
def left_outer_join(rdd1, rdd2, num_partitions=10):
    rdd1 = rdd1.map(lambda x: (x[0], (x[1], 'L')))
    rdd2 = rdd2.map(lambda x: (x[0], (x[1], 'R')))

    rdd_union = rdd1.union(rdd2).groupByKey(numPartitions=num_partitions)

    def flat_map(s):
        iterator = s[1]
        left = 'NA'
        right_set = set()
        for data in iterator:
            if data[1] == 'L':
                left = data[0]
            else:
                right_set.add(data[0])
        result = list()
        for right in right_set:
            result.append((left, right))
        return result

    def flat_map_values(iterator):
        right_set = set()
        for data in iterator:
            right_set.add(data)
        return right_set

    return rdd_union.flatMap(flat_map).groupByKey(numPartitions=num_partitions).flatMapValues(flat_map_values)


def find_common_friends(rdd):
    def map_with_friend_pair_key(s):
        person = s[0]
        friend_list = s[1]

        result = list()
        for friend in friend_list:
            person1 = person
            person2 = friend
            if person1 > person2:
                temp = person1
                person1 = person2
                person2 = temp
            key = person1 + ' and ' + person2
            result.append((key, friend_list))
        return result

    def reduce_by_key(iterator1, iterator2):
        all_friends = set()
        for friend in iterator1:
            all_friends.add(friend)

        common_friends = set()
        for friend in iterator2:
            if friend in all_friends:
                common_friends.add(friend)
        return common_friends

    return rdd.flatMap(map_with_friend_pair_key).reduceByKey(reduce_by_key)
