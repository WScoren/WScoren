from pyspark import SparkContext,SparkConf
import os

os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'
os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python"

os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

if __name__ == '__main__':
    conf=SparkConf().setAppName('test').setMaster('local[*]')
    sc=SparkContext(conf=conf)

    rdd1=sc.parallelize([i for i in range(10,21)])

    def func(partitionData):
        temp=[]
        for i in partitionData:
            temp.append(i*2)
        return iter(temp)

    #map是对每个数据进行操作的,比如一个partition中有1万条数据,那么传入的函数就要执行1万次
    rdd2=rdd1.map(lambda x:x*2)
    #结果: [20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40]

    # mapPartition是对rdd中每个分区的迭代器进行操作,也就说一个task仅执行一次传入的函数,
    # 传入的函数一次性接受所有的partition数据,只需要执行一次就好,所以性能较高,
    # 但是也可能会引起OOM(内存溢出)
    rdd3=rdd1.mapPartitions(func)
    #结果: [20, 22, 24, 26, 28, 30, 32, 34, 36, 38, 40]

    def func1(x):
        temp=[]
        for i in x:
            print(i)
            temp.append(i*2)
        return temp
    rdd5=sc.parallelize([[1,2],[2,3]])

    rdd6=rdd5.flatMap(func1)
    rdd6.collect()

    #使用map对rdd5进行操作的话,
    #结果:[[1, 2, 1, 2], [2, 3, 2, 3]]
    rdd7=rdd5.map(lambda x:x*2)
    rdd7.collect()


