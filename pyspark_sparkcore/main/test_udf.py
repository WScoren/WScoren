from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
import os

os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'
os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python"

os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON

# Apache Arrow 是一种内存中的列式数据格式，用于 Spark 中以在 JVM 和 Python 进程之间有效地传输数据。
# 需要安装Apache Arrow，pip install pyspark[sql]  -i https://pypi.tuna.tsinghua.edu.cn/simple
# 使用：spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# 2-开启pyarrow，能加快计算速度。原理有2个：1-基于内存减少了序列化和反序列化开销，2-基于矢量(向量)计算vectorize

# 3-创建pandas的DataFrame

import pandas as pd
if __name__ == '__main__':
    # 1.创建SparkSession上下文对象
    # 设置参数的第一种语法,用config
    spark=SparkSession.builder\
        .appName('test1')\
        .master('local[*]')\
        .config('spark.sql.shuffle.partitions','4')\
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    df_pd=pd.DataFrame({
        'integer':[1,2,3],
        'floats':[-1.0,0.6,2.6],
        'integer_arrays':[[1,2],[3,4,5],[5,6,7,8]]
    })

    df=spark.createDataFrame(df_pd)
    df.printSchema()
    df.show()

    from pyspark.sql.functions import udf
    from pyspark.sql.types import IntegerType,FloatType,ArrayType,StructType

    udf1=udf(lambda x:x**2,IntegerType())

    #使用udf1
    df.select(
        '*',
        udf1('integer').alias('myint')
    ).show()

