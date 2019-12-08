from pyspark.sql import SparkSession
import argparse
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("-y", "--years", nargs="+", required="true", help="List of years to combine data for. 2016, 2017, and 2018 are available on my repo")
# parser.add_argument("-a", "--atbats", help="minimum at bats required to consider a player (default 0)", default=0, type=int)
# parser.add_argument("-s", "--sort", help="which stat to sort by (default RC)", default="RC", choices=["RC", "RC27"])
# parser.add_argument("-p", "--players", help="number of players to display (with highest RC/RC27) (0 for all, default all)", default=0, type=int)
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
  res = urllib.request.urlopen(url)
  if res.getcode() == 200:
    newData = res.read().decode("utf-8")
    if idx == 0:
      csvString = newData
    else:
      csvString += "\n" + newData.partition("\n")[2]
  else:
    print("Year file not found!")

# write to an RDD then a DataFrame because I'm not sure how to write to HDFS outside of that
csvDF = spark.sparkContext.parallelize(csvString.splitlines())
pitchingDF = spark.read.csv(csvDF, header=True)
pitchingDF.write.csv("C:\\Users\\Vincent\\pyspark-scripts\\bushong_phase3_combined.csv", mode="overwrite", header="true")
