from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer

sc=SparkContext(appName="PythonStreamingKafkaWordCount")
#sc.setLogLevel("ERROR")

def getSparkContext():
    return sc

def getModel():
    sc =getSparkContext()
    sqlContext = SQLContext(sc)
    data = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load('C:/Users/kgasw/PycharmProjects/bigdata_project/twitter_traffic_data_static.csv')
    regexTokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W")
    stopwordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered")
    countVectors = CountVectorizer(inputCol="filtered", outputCol="features", vocabSize=10000, minDF=5)
    label_stringIdx = StringIndexer(inputCol = "class", outputCol = "label")
    pipeline = Pipeline(stages=[regexTokenizer, stopwordsRemover, countVectors, label_stringIdx])
    pipelineFit = pipeline.fit(data)
    dataset = pipelineFit.transform(data)
    (trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed = 100)
    print("Training Dataset Count: " + str(trainingData.count()))
    print("Test Dataset Count: " + str(testData.count()))
    lr = LogisticRegression(maxIter=20, regParam=0.3, elasticNetParam=0)
    lrModel = lr.fit(trainingData)
    predictions = lrModel.transform(testData)
    predictions.filter(predictions['prediction'] == 1) \
        .select("text","class","probability","label","prediction") \
        .orderBy("probability", ascending=False) \
        .show(n = 10, truncate = 30)
    evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
    print(evaluator.evaluate(predictions))
    return lrModel,pipelineFit