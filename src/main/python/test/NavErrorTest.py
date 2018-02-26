import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext

from com.iai.NavigationErrorEventHandler import handle

from com.iai.Configuration import Configuration

from com.iai.NavigationErrorEventHandler import add_to_hist
from com.iai.NavigationErrorEventHandler import merge_hist
#conf = Configuration()

#sc = SparkContext(conf.getProperty("spark","master"), conf.getProperty("spark","app.name"))

#text_file = sc.textFile('D:\\pythonProjects\\pythonKafkaStreaming\\src\\main\\resources\\test_data\\data_example.json')

#print(text_file.collect())
#ssc = StreamingContext(sc, 1)
#stream = ssc.socketTextStream("127.0.0.1", 9999)
#stream.pprint()

hist1 = {2.0:2}
hist2 = {2.0:2,1.0:5}

hist = [{"nav":22,"x":3},{"nav":32,"x":3},{"nav":12,"x":3},{"nav":13,"x":3},{"x":3}]

def get_nav_total_error(e):
    if (e.has_key('nav')):
        return e['nav']
    else:
        return None


print(merge_hist(hist2,hist2))

max_error = max(hist,  key=lambda e: get_nav_total_error(e))

print(max_error)

#handle(stream.map(lambda o: json.loads(o))).pprint()

#stream = ssc.textFileStream(directory="D:\\pythonProjects\\pythonKafkaStreaming\\src\\main\\resources\\test_data\\")

#stream.pprint()

#ssc.start()
#ssc.awaitTermination()