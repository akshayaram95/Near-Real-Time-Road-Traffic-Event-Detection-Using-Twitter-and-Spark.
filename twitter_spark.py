from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
import pyspark.sql as sql
from model_building import getModel,getSparkContext

model,pipelineFit = getModel()

def formatData(jsonString):
    return jsonString[1]

def process(time, rdd):
    data = spark.read.json(rdd)
    if data.count() > 0 :
        dataset = pipelineFit.transform(data)
        predictions = model.transform(dataset)
        predicted_traffic_data = predictions.filter(predictions['prediction'] == 1) \
            .select("text", "created_at", "geo", "coordinates")
    #rdd.foreach(index_to_elasticsearch)
    
if __name__ == "__main__":
    sc = getSparkContext()
    spark = sql.SparkSession(sc)
    sqlContext = SQLContext(sc)
    ssc = StreamingContext(sc, 1)
    topic='trafficnyc'
    zkQuorum='localhost:2181'
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(formatData)
    lines.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()