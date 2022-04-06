import os
from pyspark.sql import SparkSession

from pyspark.sql import functions as F


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

    sc=spark.sparkContext

    rdd1=sc.textFile('file:///export/pysaprk_learn/pyspark_sparksql/data/score.txt')

    # def score_info(x):
    #     return (x.split(',')[0],int(x.split(',')[1]),x.split(',')[2],int(x.split(',')[3]))
    #
    # rdd2=rdd1.map(score_info)
    # # rdd2.foreach(lambda x:print(x))
    #
    # df=rdd2.toDF(['name','age','sex','score'])

    data=[
        ['zhangsan',10,None,50],
        ['lisi',None,'女',60],
        [None,12,'男',90],
        ['chenyi',11,'女',None],
        ['chenhai',9,None,64]
    ]
    df=spark.createDataFrame(data,schema=['name','age','sex','score'])

    # df.select(['sex']).show()

    #填充sex为null的数据为'男'
    df = df.fillna('男', subset='sex')

    #删除name为空的记录
    df = df.dropna(subset=['name'])

    #获取age,score列平均值


