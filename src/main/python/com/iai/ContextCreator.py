import json

from datetime import datetime
from pyspark import SparkContext, RDD
from pyspark.streaming import StreamingContext, DStream
from pyspark.streaming.kafka import KafkaUtils

from com.iai.MainHandler import handle
from com.iai.Configuration import Configuration


system_start_time = datetime.now()
globals()['system_start'] = system_start_time

conf = Configuration()

sc = SparkContext(conf.getProperty("spark","master"), conf.getProperty("spark","app.name"))

ssc = StreamingContext(sc, int(conf.getProperty("streaming.context","duration")))
kafkaReceiverParams = {
    "metadata.broker.list": conf.getProperty("streaming.context","metadata.broker.list"),
    "auto.offset.reset": conf.getProperty("streaming.context","auto.offset.reset"),
    "group.id": conf.getProperty("streaming.context","group.id"),
    "client.id": conf.getProperty("streaming.context","client.id")}



ssc.checkpoint(conf.getProperty("streaming.context","checkpoint.dir"))

kafkaStream = KafkaUtils.createDirectStream(ssc, [conf.getProperty("streaming.context","topic")], kafkaReceiverParams)

kafkaStream.pprint()
#mainHandler = MainHandler()
#mainHandler.handle(kafkaStream)
handle(kafkaStream.map(lambda o: json.loads(o[1])))

ssc.start()
ssc.awaitTermination()
