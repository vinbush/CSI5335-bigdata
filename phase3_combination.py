from pyspark.sql import SparkSession
import urllib.request

pitchingUrl = "https://raw.githubusercontent.com/vinbush/CSI5335-bigdata/master/2017_pitching.csv"

spark = SparkSession.builder \
        .master("local") \
        .appName("baseball") \
        .getOrCreate()

res = urllib.request.urlopen(pitchingUrl)

data = res.read()

print(data)

#batting = spark.read.csv(pitchingUrl, header=True)

#batting.show()
