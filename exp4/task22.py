from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化Spark会话
spark = SparkSession.builder.appName("AverageDailyIncome").getOrCreate()

# 读取数据
df = spark.read.csv("application_data.csv", header=True, inferSchema=True)

# 数据转换：计算每日平均收入
df_with_avg_income = df.withColumn("avg_income", col("AMT_INCOME_TOTAL") / -col("DAYS_BIRTH"))

# 过滤出平均日收入大于1的客户并排序
df_filtered = df_with_avg_income.filter(col("avg_income") > 1).orderBy(col("avg_income").desc())

# 选择需要的列
df_final = df_filtered.select("SK_ID_CURR", "avg_income")

# 展示结果
df_final.show()

# 保存为CSV
df_final.write.csv("average_daily_income.csv", header=True)

# 停止Spark会话
spark.stop()
