from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from datetime import datetime
from pyspark.sql import functions as sf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
import sys

### getMlSuitableDataFrame ###
# a procedure to get a dataframe suitable for regression calculation for teams for a specific year
# only in a function so I don't have to copy this for the test year
def getMlSuitableDataFrame(year):
  # for local filesystem testing
  # battingFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Batting.csv"
  # teamsFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Teams.csv"

  # for local HDFS testing
  # battingFile = "hdfs://localhost:9000/user/baseball/Batting.csv"
  # teamsFile = "hdfs://localhost:9000/user/baseball/Teams.csv"

  # for submission
  battingFile = "hdfs://localhost:8020/user/baseball/Batting.csv"
  teamsFile = "hdfs://localhost:8020/user/baseball/Teams.csv"

  spark = SparkSession.builder \
          .master("local") \
          .appName("baseball") \
          .getOrCreate()

  batting = spark.read.csv(battingFile, header=True)
  teamsBpf = spark.read.csv(teamsFile, header=True).select('teamID', 'yearID', 'BPF')

  # join batter data with the team id
  battingWithBpf = batting.filter(batting['yearID'] == year).join(teamsBpf.filter(teamsBpf['yearID'] == year), 'teamID')
  # battingWithBpf = batting.filter(batting['yearID'] == trainingYear).join(teamsBpf.filter(teamsBpf['yearID'] == trainingYear), 'teamID')

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

  # Reorganize equation to consider three terms 
  teamBatting = (teamBatting
    .withColumn('B', (teamBatting['TotalBases'] * teamBatting['OnBase'] / teamBatting['Opportunities']))
    .withColumn('C', (teamBatting['AdjustedWalks'] * teamBatting['OnBase'] / teamBatting['Opportunities']))
    .withColumn('D', (teamBatting['SacrificesSteals'] * teamBatting['OnBase'] / teamBatting['Opportunities']))
  )

  # Adjust runs for park factor
  teamBatting = (teamBatting
    .withColumn('AdjustedRuns', teamBatting['R'] / (((teamBatting['BPF'] / 100) + 1) / 2))
  )

  # create a vector of the three terms and project down to it and the adjusted runs columns
  vectorAssembler = VectorAssembler(inputCols = ['B', 'C', 'D'], outputCol = 'features')
  vbatting = vectorAssembler.transform(teamBatting)
  vbatting = vbatting.select(['features', 'AdjustedRuns'])
  return vbatting
### end getMlSuitableDataFrame ###

trainingYear = 2017 # use 2017 for weight calculation
testYear = 2018 # 2018 for testing the model

trainingData = getMlSuitableDataFrame(trainingYear)
testingData = getMlSuitableDataFrame(testYear)

# set up the linear regression
lr = LinearRegression(featuresCol = 'features', labelCol='AdjustedRuns', maxIter=10, regParam=0.3, elasticNetParam=0.8)
lr.setFitIntercept(False) # if all the given stats are zero, runs created should be zero

# apply the regression
lr_model = lr.fit(trainingData)

# output results
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept (manually set to zero): " + str(lr_model.intercept))

print("\n***Training data (2017)***")

# show original data characteristics
print("Original data:")
trainingData.describe().show()

# show RMSE and r squared on training data
print("Training data results:")
trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)

print("\nTraining data predictions:")
trainingSummary.predictions.show(30)
print("\nTraining data residuals:")
trainingSummary.residuals.show(30)
print("\nTraining data season runs prediction:")
trainingSummary.predictions.agg({'prediction': 'sum'}).show()

# test the regression
testingSummary = lr_model.evaluate(testingData)

print("\n***Testing data (2018)***")

# show original data characteristics
print("Original data:")
testingData.describe().show()

print("RMSE: %f" % testingSummary.rootMeanSquaredError)
print("r2: %f" % testingSummary.r2)

print("\nTesting data predictions:")
testingSummary.predictions.show(30)
print("\nTesting data residuals:")
testingSummary.residuals.show(30)
print("\nTesting data season runs prediction:")
testingSummary.predictions.agg({'prediction': 'sum'}).show()
