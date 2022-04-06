from pyspark.sql import SparkSession,Row

import os


os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
if __name__ == '__main__':
    #1-创建SparkSession上下文对象
    spark=SparkSession.builder\
        .appName('test1')\
        .master('local[*]')\
        .getOrCreate()

    #2.获取SparkContext对象,用来加载文件形成RDD
    sc=spark.sparkContext

    rdd1=sc.textFile('file:///export/pysaprk_learn/pyspark_sparksql/data/people.txt')

    #打印rdd1中的每个元素
    # rdd1.foreach(lambda x:print(x))

    #3.将RDD的每个元素转换成Row对象
    rdd_row=rdd1.map(lambda x:Row(name=x.split(',')[0],age=int(x.split(',')[1].strip())))
    rdd_row.foreach(lambda x:print(x))

    #4.将RDD转换成DataFrame
    df=spark.createDataFrame(rdd_row)

    #打印schema信息
    df.printSchema()
    df.show()

    #5.对DataFrame进行etl分析,查询年龄13~19岁的元素
    df.createOrReplaceTempView('people')
    df2=spark.sql('select * from people where age>=13 and age<=19')

    df2.show()

    #6.关闭上下文对象
    spark.stop()



