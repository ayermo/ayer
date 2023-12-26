from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 初始化Spark
spark = SparkSession.builder.appName("LoanDefaultPrediction").getOrCreate()

# 读取数据
data = spark.read.csv("application_data4.csv", header=True, inferSchema=True)

# 数据预处理（示例）
# 这里需要根据你的数据选择合适的处理方式
# 例如，选择特征、处理缺失值、转换类别特征等

# 数据分割
train_data, test_data = data.randomSplit([0.8, 0.2])

# 特征工程
feature_columns =['NAME_CONTRACT_TYPE','AMT_INCOME_TOTAL','AMT_CREDIT','NAME_INCOME_TYPE','NAME_EDUCATION_TYPE','NAME_FAMILY_STATUS','FLAG_CONT_MOBILE','REGION_RATING_CLIENT','REG_REGION_NOT_LIVE_REGION','ORGANIZATION_TYPE','OBS_30_CNT_SOCIAL_CIRCLE','DEF_30_CNT_SOCIAL_CIRCLE','OBS_60_CNT_SOCIAL_CIRCLE','DEF_60_CNT_SOCIAL_CIRCLE','FLAG_DOCUMENT_2','FLAG_DOCUMENT_3','FLAG_DOCUMENT_14'  ]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
# 模型
lr = LogisticRegression(labelCol="TARGET", featuresCol="features")

# 构建Pipeline
pipeline = Pipeline(stages=[assembler, lr])

# 模型训练
model = pipeline.fit(train_data)

# 模型评估
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="TARGET", rawPredictionCol="rawPrediction")
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})

# 输出准确率
print("Accuracy: ", accuracy)

# 关闭Spark
spark.stop()
