from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# 初始化Spark会话
spark = SparkSession.builder.appName("LoanAmountDistribution").getOrCreate()

# 读取CSV文件
df = spark.read.csv("application_data.csv", header=True, inferSchema=True)

# 定义区间函数
def get_credit_interval(amt_credit):
    interval_start = int(amt_credit // 10000 * 10000)
    interval_end = interval_start + 10000
    return f"({interval_start},{interval_end})"

# 注册UDF
interval_udf = udf(get_credit_interval, StringType())

# 应用区间函数
df_with_interval = df.withColumn("credit_interval", interval_udf(df["AMT_CREDIT"]))

# 分组和计数
result = df_with_interval.groupBy("credit_interval").count()

# 显示结果
result.show()

# 关闭Spark会话
spark.stop()
