from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 初始化Spark会话
spark = SparkSession.builder.appName("ChildrenCountStats").getOrCreate()

# 读取数据
df = spark.read.csv("application_data.csv", header=True, inferSchema=True)

# 创建视图
df.createOrReplaceTempView("application_data")

# 执行SQL查询
query = """
SELECT 
    CNT_CHILDREN, 
    COUNT(*) / (SELECT COUNT(*) FROM application_data WHERE CODE_GENDER = 'M') AS ratio
FROM 
    application_data
WHERE 
    CODE_GENDER = 'M'
GROUP BY 
    CNT_CHILDREN
"""

# 运行查询
result = spark.sql(query)

# 展示结果
result.show()

# 停止Spark会话
spark.stop()
