from pyspark.sql import SparkSession #sparkSession为同统一入口

#创建spakr对象
spark = SparkSession\
    .builder\
    .appName('readfile')\
    .getOrCreate()

# 1.读取csv文件
# 1.读取csv文件
logFilePath = "file:///home/mls/PycharmProjects/BigDataProject/1.1.csv"
log_df = spark.read.csv(logFilePath,
                        encoding='utf-8',
                        header=False,
                        inferSchema=True,
                        sep=',')

log_df.select('_c0').distinct().orderBy('_c0').collect()
# '_c0' col('_c0') log_df['_c0']
a = log_df.select('_c0').distinct().orderBy('_c0').collect()
log_df.selectExpr("_c0 as area","_c1 as confirmed","_c2 as died","_c3 as crued","_c4 as curConfirm","_c5 as confirmedRelative").show()
print(a)
log_df.describe().selectExpr("_c0 as area","_c1 as confirmed","_c2 as died","_c3 as crued","_c4 as curConfirm","_c5 as confirmedRelative").show()