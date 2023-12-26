from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化Spark会话
spark = SparkSession.builder.appName("LoanIncomeDifference").getOrCreate()

# 读取CSV文件
df = spark.read.csv("application_data.csv", header=True, inferSchema=True)

# 计算差值
df_with_diff = df.withColumn("difference", col("AMT_CREDIT") - col("AMT_INCOME_TOTAL"))

# 按差值降序排序并取前10条记录
highest_diff = df_with_diff.orderBy(col("difference").desc()).select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "difference").limit(10)

# 按差值升序排序并取前10条记录
lowest_diff = df_with_diff.orderBy(col("difference")).select("SK_ID_CURR", "NAME_CONTRACT_TYPE", "AMT_CREDIT", "AMT_INCOME_TOTAL", "difference").limit(10)

# 显示结果
print("Highest Difference:")
highest_diff.show(truncate=False)

print("Lowest Difference:")
lowest_diff.show(truncate=False)

# 关闭Spark会话
spark.stop()