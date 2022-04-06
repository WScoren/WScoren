
import decimal
import os
import string

from pyspark.sql import SparkSession

os.environ['JAVA_HOME'] = '/export/server/jdk1.8.0_241'
os.environ['SPARK_HOME'] = '/export/server/spark'
PYSPARK_PYTHON = "/root/anaconda3/bin/python3.8"
# 当存在多个版本时，不指定很可能会导致出错
os.environ["PYSPARK_PYTHON"] = PYSPARK_PYTHON
os.environ["PYSPARK_DRIVER_PYTHON"] = PYSPARK_PYTHON


#Spark工具方法
#如果python代码和SQL代码耦合在一起，那么可读性差。可以用工具方法将python代码和SQL代码分离开。让我们更专注SQL业务。
def executeSQLFile(filename):
 with open('/export/pysaprk_learn/pyspark_sparksql/newcode_spl/'+filename) as f:
     read_data = f.read()
 # 对内容用分号截取，划分成多个SQL语句
 arr = read_data.split(';')
 for sql in arr:
     # print(sql,';')
     # 如果sql文本字符串中，既有注释又有有效的SQL语句，那么送给spark.sql("...")混合可以运行。
     # 但是如果sql文本字符串内容只有注释却没有有效的SQL语句，那么执行送给spark.sql("--...")会报错。
     # 将SQL语句的注释的行，过滤掉。
     filtered = filter(lambda line: len(line.strip()) > 0 and not line.strip().startswith('--'), sql.splitlines())
     ## 只有有效的SQL语句的行,没有注释. 如果有效的SQL语句存在，就执行。如果只有注释却没有有效的SQL语句，就不执行。
     list2 = list(filtered)
     if (len(list2) > 0):
         df = spark.sql(sql)
         # 如果有效SQL语句是以select开头，那么顺便打印数据。
         if (list2[0].strip().lower().startswith('select')):
             df.show()

if __name__ == '__main__':
    # 1-创建上下文对象
    spark = SparkSession.builder\
        .appName('test') \
        .master('local[*]') \
        .config('hive.metastore.uris','thrift://node1:9083') \
        .config('spark.sql.warehouse.dir','/user/hive/warehouse') \
        .enableHiveSupport() \
        .getOrCreate()

    #步骤4
    #开发一个pandas_udf来计算
    from pyspark.sql.functions import pandas_udf
    import pandas as pd

    @pandas_udf('int')
    def distinct_pid(product_id:pd.Series) ->int:
        temp_set=set()
        for i in product_id:
            # print(i)
            temp_set.add(i)
        return len(temp_set)
    spark.udf.register('distinct_pid',distinct_pid)

    executeSQLFile('newconde_18.sql')









