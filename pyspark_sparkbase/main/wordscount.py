from pyspark import SparkConf, SparkContext
import os
# 这里可以选择本地PySpark环境执行Spark代码，也可以使用虚拟机中PySpark环境，通过os可以配置
os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON
if __name__ == '__main__':
    #1-首先创建SparkContext上下文环境
    conf=SparkConf().setAppName('wordcount').setMaster('local[*]')
    #2从外部文件数据源读取数据
    sc=SparkContext(conf=conf)
    rdd1=sc.textFile('file:///export/pysaprk_learn/pyspark_sparkbase/data/words.txt')

    #3.执行flatmap,执行扁平化操作
    rdd2=rdd1.flatMap(lambda line:line.split(' '))

    print(rdd2.collect())
    rdd2_1=rdd1.map(lambda line:line.split(' '))
    print(rdd2_1.collect())

    #4.执行map转化操作,得到(word,1)
    rdd3=rdd2.map(lambda word:(word,1))

    #5.reduceByKey将相同Key的Value数据累加操作
    rdd4=rdd3.reduceByKey(lambda x,y:x+y)
    print(rdd4.collect())

    #6.将结果输出到文件系统
    # rdd4.saveAsTextFile('file:///export/pysaprk_learn/pyspark_sparkbase/data/word.txt')




