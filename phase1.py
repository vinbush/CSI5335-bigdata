from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from datetime import datetime
import argparse
import sys

def runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
	timesOnBase = H + BB - CS + HBP - GIDP
	basesAdvanced = (B1 + 2*B2 + 3*B3 + 4*HR) + .26*(BB - IBB + HBP) + .52*(SH + SF + SB)
	opportunities = AB + BB + HBP + SF + SH
	if opportunities == 0:
		return 0
	return ((timesOnBase * basesAdvanced) / opportunities) / (((BPF / 100) + 1) / 2)

def runsCreated27(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
	outs = AB - H + SF + SH + GIDP + CS
	if outs == 0:
		return 0
	return (runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF) * 27) / outs

def rcPairAdder(x, y):
	return ((x[0] + y[0]), (x[1] + y[1]))

parser = argparse.ArgumentParser()
parser.add_argument("year", help="The year to calculate RC for")
parser.add_argument("-a", "--atbats", help="minimum at bats required to consider a player (default 0)", default=0, type=int)
parser.add_argument("-s", "--sort", help="which stat to sort by (default RC)", default="RC", choices=["RC", "RC27"])
parser.add_argument("-p", "--players", help="number of players to display (with highest RC/RC27) (0 for all, default all)", default=0, type=int)
args = parser.parse_args()

if args.atbats < 0:
	parser.error("Minimum at bats must not be negative")

if args.players < 0:
	parser.error("Specified player count must not be negative")

if args.sort != "RC" and args.sort != "RC27":
	parser.error("Must sort by RC or RC27")

year = args.year
minAB = args.atbats
sort = args.sort
sortIndex = 1 if sort == "RC" else 2
players = args.players

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

# filter minimum at-bats, if specified
if minAB > 0:
	battingWithBpf = battingWithBpf.filter(battingWithBpf['AB'] >= minAB)

# turns this dataframe into an RDD of Row objects
playerRcBpf = (battingWithBpf.rdd
	# maps the RDD into <k, v> where k is player ID and v is a tuple (RC, RC27)
	.map(lambda r:
		(
			r['playerID'],
			(round(runsCreated(r['AB'], r['H'], (r['1B']),
				r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
				r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['BPF']), 2),
			round(runsCreated27(r['AB'], r['H'], (r['1B']),
				r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
				r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['BPF']), 2))
		)
	)
	# sum the RC and RC27 by player
	.reduceByKey(rcPairAdder)
	# expand out the RC, RC27 tuple (so it maps easier to a dataframe for export)
	.map(lambda r: (r[0], r[1][0], r[1][1]))
	# sort by RC or RC27
	.sortBy(lambda r: r[sortIndex], ascending=False)
)

output = spark.createDataFrame(playerRcBpf)

if players > 0:
	output = output.limit(players)

# for local filesystem testing
# output.write.csv("C:\\Users\\Vincent\\pyspark-scripts\\bushong_phase1_" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")

# for local HDFS testing
# output.write.csv("hdfs://localhost:9000/user/bushong/BD/bushong_phase1_" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")

# for submission
output.write.csv("hdfs://localhost:8020/user/bushong/BD/bushong_phase1_" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")

