from pyspark.sql import SparkSession
import argparse
import sys
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("-y", "--years", nargs="+", required="true", help="List of years to combine data for. 2015, 2016, 2017, and 2018 are currently available on my repo. For example: -y 2015 2016 2017 2018")
args = parser.parse_args()

years = args.years


baseUrl = "https://raw.githubusercontent.com/vinbush/CSI5335-bigdata/master/pitching_years/"

spark = SparkSession.builder \
        .master("local") \
        .appName("baseball") \
        .getOrCreate()

csvString = ""

for idx, year in enumerate(years):
  url = baseUrl + year + "_pitching.csv"
  try:
    res = urllib.request.urlopen(url)
  except:
    print("Error while fetching year " + year + "! Currently, only 2015-2018 are available.")
  newData = res.read().decode("utf-8")
  if idx == 0:
    csvString = newData
  else:
    csvString += "\n" + newData.partition("\n")[2]

# write to an RDD then a DataFrame because I'm not sure how to write to HDFS outside of that
csvDF = spark.sparkContext.parallelize(csvString.splitlines())
pitchingDF = spark.read.csv(csvDF, header=True)

# for local filesystem testing
# pitchingDF.write.csv("C:\\Users\\Vincent\\pyspark-scripts\\bushong_phase3_combined.csv", mode="overwrite", header="true")

# for local HDFS testing
# pitchingDF.write.csv("hdfs://localhost:9000/user/bushong/DS/bushong_phase3_combined.csv", mode="overwrite", header="true")

# for submission
pitchingDF.write.csv("hdfs://localhost:8020/user/bushong/DS/bushong_phase3_combined.csv", mode="overwrite", header="true")
