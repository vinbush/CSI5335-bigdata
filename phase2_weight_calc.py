from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from datetime import datetime
from pyspark.sql import functions as sf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import sys

trainingYear = 2017 # use 2017 for weight calculation

battingFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Batting.csv"
teamsFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Teams.csv"

spark = SparkSession.builder \
        .master("local") \
        .appName("baseball") \
        .getOrCreate()

batting = spark.read.csv(battingFile, header=True)
teamsBpf = spark.read.csv(teamsFile, header=True).select('teamID', 'yearID', 'BPF')

# join batter data with the team id
battingWithBpf = batting.filter(batting['yearID'] == trainingYear).join(teamsBpf.filter(teamsBpf['yearID'] == trainingYear), 'teamID')

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
	.withColumn('R', battingWithBpf['R'].cast(IntegerType()))
)

# Get team stat sums
teamBatting = battingWithBpf.groupBy('teamID').agg(
	sf.sum('AB').alias('AB'),
	sf.sum('H').alias('H'),
	sf.sum('1B').alias('1B'),
	sf.sum('2B').alias('2B'),
	sf.sum('3B').alias('3B'),
	sf.sum('HR').alias('HR'),
	sf.sum('BB').alias('BB'),
	sf.sum('IBB').alias('IBB'),
	sf.sum('HBP').alias('HBP'),
	sf.sum('SF').alias('SF'),
	sf.sum('SH').alias('SH'),
	sf.sum('GIDP').alias('GIDP'),
	sf.sum('SB').alias('SB'),
	sf.sum('CS').alias('CS'),
	sf.max('BPF').alias('BPF'),
	sf.sum('R').alias('R')
)

# Create helper columns to make below calculations cleaner
teamBatting = (teamBatting
	.withColumn('OnBase', teamBatting['H'] + teamBatting['BB'] - teamBatting['CS'] + teamBatting['HBP'] - teamBatting['GIDP'])
	.withColumn('TotalBases', teamBatting['1B'] + 2 * teamBatting['2B'] + 3 * teamBatting['3B'] + 4 * teamBatting['HR'])
	.withColumn('AdjustedWalks', teamBatting['BB'] - teamBatting['IBB'] + teamBatting['HBP'])
	.withColumn('SacrificesSteals', teamBatting['SH'] + teamBatting['SF'] + teamBatting['SB'])
	.withColumn('Opportunities', teamBatting['AB'] + teamBatting['BB'] + teamBatting['HBP'] + teamBatting['SF'] + teamBatting['SH'])
)

# # Reorganize equation to consider three terms 
# teamBatting = (teamBatting
# 	.withColumn('B', teamBatting['TotalBases'] * teamBatting['OnBase'] / teamBatting['Opportunities'])
# 	.withColumn('C', teamBatting['AdjustedWalks'] * teamBatting['OnBase'] / teamBatting['Opportunities'])
# 	.withColumn('D', teamBatting['SacrificesSteals'] * teamBatting['OnBase'] / teamBatting['Opportunities'])
# )

# Reorganize equation to consider three terms 
teamBatting = (teamBatting
	.withColumn('B', (teamBatting['TotalBases'] * teamBatting['OnBase'] / teamBatting['Opportunities']) / (((teamBatting['BPF'] / 100) + 1) / 2))
	.withColumn('C', (teamBatting['AdjustedWalks'] * teamBatting['OnBase'] / teamBatting['Opportunities']) / (((teamBatting['BPF'] / 100) + 1) / 2))
	.withColumn('D', (teamBatting['SacrificesSteals'] * teamBatting['OnBase'] / teamBatting['Opportunities']) / (((teamBatting['BPF'] / 100) + 1) / 2))
)

vectorAssembler = VectorAssembler(inputCols = ['B', 'C', 'D'], outputCol = 'features')
vbatting = vectorAssembler.transform(teamBatting)
vbatting = vbatting.select(['features', 'R'])

lr = LinearRegression(featuresCol = 'features', labelCol='R', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(vbatting)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

vbatting.describe().show()

# print(battingWithBpf.groupBy('teamID').agg({
# 	'AB': 'sum',
# 	'H': 'sum',
# 	'1B': 'sum',
# 	'2B': 'sum',
# 	'3B': 'sum',
# 	'HR': 'sum',
# 	'BB': 'sum',
# 	'IBB': 'sum',
# 	'HBP': 'sum',
# 	'SF': 'sum',
# 	'SH': 'sum',
# 	'GIDP': 'sum',
# 	'SB': 'sum',
# 	'CS': 'sum',
# 	'BPF': 'max'
# }).collect())



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
