from pyspark import SparkContext, SparkConf



conf = SparkConf().setMaster("local").setAppName("readtxt")
sc = SparkContext.getOrCreate();




# 读取多个数据文件,rdd内容是每一行

lines = sc.textFile("file:///home/mls/PycharmProjects/BigDataProject/1228.csv")
lines_a = sc.textFile("file:///home/mls/PycharmProjects/BigDataProject/1229.csv")
lines_b = sc.textFile("file:///home/mls/PycharmProjects/BigDataProject/1230.csv")
lines_c = sc.textFile("file:///home/mls/PycharmProjects/BigDataProject/1.1.csv")

#将所有统计表联系起来
count_rdd = lines.union(lines_a) .union(lines_b).union(lines_c)
print(lines.count(), lines.collect())


# 把每一行（字符串），转换成相应项（6个字段）
item_rdd = lines.map(lambda line: line.split(','))
print(item_rdd.count(), item_rdd.collect())
l = item_rdd.collect()
item_rdd1 = item_rdd.map(lambda item: (item[0], (int(item[1]), int(item[2]), int(item[3]), int(item[4]), int(item[5]))))
print(item_rdd1.count(), item_rdd1.collect())

#降序排列 找出确诊数最多的
item_rdd2 = item_rdd1.sortBy(keyfunc = (lambda item: item[1]), ascending=False)
print(item_rdd2.count(), item_rdd2.collect())

#升序排列找出确诊最少的
item_rdd3 = item_rdd1.sortBy(keyfunc = (lambda item: item[1]))
print(item_rdd3.count(), item_rdd3.collect())

# rank_rdd = item_rdd3.groupByKey().mapValues(lambda v : list(v))
# print(rank_rdd.count() , rank_rdd.collect())
# #
# subject_sort_rdd = rank_rdd.mapValues(lambda theList : sorted(theList, key=lambda student : student[1] , reverse= True))
# print(subject_sort_rdd.count() , subject_sort_rdd.collect())

# print(subject_sort_rdd.keys().collect())

sc.stop()
