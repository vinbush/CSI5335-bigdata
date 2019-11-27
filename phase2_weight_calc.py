from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from datetime import datetime
import sys

year = 2017 # use 2017 for weight calculation

battingFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Batting.csv"
teamsFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Teams.csv"

spark = SparkSession.builder \
        .master("local") \
        .appName("baseball") \
        .getOrCreate()

batting = spark.read.csv(battingFile, header=True)
teamsBpf = spark.read.csv(teamsFile, header=True).select('teamID', 'yearID', 'BPF')

# join batter data with the team id
battingWithBpf = batting.filter(batting['yearID'] == year).join(teamsBpf.filter(teamsBpf['yearID'] == year), 'teamID')

# column changes: calculate singles, and convert everything to integers
battingWithBpf = (battingWithBpf
	.withColumn('1B', battingWithBpf['H'] - battingWithBpf['2B'] - battingWithBpf['3B'] - battingWithBpf['HR'])
	.withColumn('AB', battingWithBpf['AB'].cast(IntegerType()))
	.withColumn('H', battingWithBpf['H'].cast(IntegerType()))
	.withColumn('2B', battingWithBpf['2B'].cast(IntegerType()))
	.withColumn('3B', battingWithBpf['3B'].cast(IntegerType()))
	.withColumn('HR', battingWithBpf['HR'].cast(IntegerType()))
	.withColumn('BB', battingWithBpf['BB'].cast(IntegerType()))
	.withColumn('IBB', battingWithBpf['IBB'].cast(IntegerType()))
	.withColumn('HBP', battingWithBpf['HBP'].cast(IntegerType()))
	.withColumn('SF', battingWithBpf['SF'].cast(IntegerType()))
	.withColumn('SH', battingWithBpf['SH'].cast(IntegerType()))
	.withColumn('GIDP', battingWithBpf['GIDP'].cast(IntegerType()))
	.withColumn('SB', battingWithBpf['SB'].cast(IntegerType()))
	.withColumn('CS', battingWithBpf['CS'].cast(IntegerType()))
	.withColumn('BPF', battingWithBpf['BPF'].cast(IntegerType()))
)

# playerRcBpf = (battingWithBpf.rdd
# 	.map(lambda r:
# 		(
# 			r['playerID'],
# 			round(runsCreated(r['AB'], r['H'], (r['1B']),
# 				r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
# 				r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['BPF']), 2),
# 			round(runsCreated27(r['AB'], r['H'], (r['1B']),
# 				r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
# 				r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['BPF']), 2)
# 		)
# 	)
# 	.sortBy(lambda r: r['playerID'])
# )

# print(playerRcBpf)

# output = spark.createDataFrame(playerRcBpf)

# #output = output.map(lambda r: ','.join([r[0], str(r[1]), str(r[2])]))
# output.write.csv("C:\\Users\\Vincent\\pyspark-scripts\\test" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")
# #output.saveAsTextFile("C:\\Users\\Vincent\\pyspark-scripts\\test" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")
