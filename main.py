from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("APIDataProcessor") \
    .getOrCreate()

# Create a StreamingContext with a batch interval of 5 seconds
ssc = StreamingContext(spark.sparkContext, 5)

# Create a DStream that fetches data from the API
data_stream = ssc.socketTextStream("wss://ws-feed.exchange.coinbase.com", 9999)

# Split the lines into words and count the frequency of each word
word_counts = data_stream.flatMap(lambda line: line.split(" ")) \
                   .map(lambda word: (word, 1)) \
                   .reduceByKey(lambda a, b: a + b)

# Print the first 10 word counts
word_counts.pprint(10)

# Start the StreamingContext
ssc.start()

ssc.awaitTermination()
