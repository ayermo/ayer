from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# 初始化Spark会话
spark = SparkSession.builder.appName("LoanDefaultPredictionWithDecisionTree").getOrCreate()

# 读取数据
data = spark.read.csv("application_data4.csv", header=True, inferSchema=True)

# 数据预处理（示例）
# ...

# 特征选择和转换
feature_columns =['NAME_CONTRACT_TYPE','AMT_INCOME_TOTAL','AMT_CREDIT','NAME_INCOME_TYPE','NAME_EDUCATION_TYPE','NAME_FAMILY_STATUS','FLAG_CONT_MOBILE','REGION_RATING_CLIENT','REG_REGION_NOT_LIVE_REGION','ORGANIZATION_TYPE','OBS_30_CNT_SOCIAL_CIRCLE','DEF_30_CNT_SOCIAL_CIRCLE','OBS_60_CNT_SOCIAL_CIRCLE','DEF_60_CNT_SOCIAL_CIRCLE','FLAG_DOCUMENT_2','FLAG_DOCUMENT_3','FLAG_DOCUMENT_14'  ]

assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
indexer = StringIndexer(inputCol="TARGET", outputCol="label")

# 数据分割
train_data, test_data = data.randomSplit([0.8, 0.2])

# 决策树模型
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")

# 构建Pipeline
pipeline = Pipeline(stages=[indexer, assembler, dt])

# 模型训练
model = pipeline.fit(train_data)

# 模型评估
predictions = model.transform(test_data)
evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction")
accuracy = evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})

# 输出准确率
print("Accuracy: ", accuracy)

# 关闭Spark会话
spark.stop()