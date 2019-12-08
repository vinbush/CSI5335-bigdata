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

pitchingFile = "C:\\Users\\Vincent\\pyspark-scripts\\pitching_combined\\bushong_phase3_combined.csv"
teamsFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Teams.csv"

spark = SparkSession.builder \
        .master("local") \
        .appName("baseball") \
        .getOrCreate()

pitching = spark.read.csv(pitchingFile, header=True)
teamsPpf = spark.read.csv(teamsFile, header=True).select('teamID', 'yearID', 'PPF') # for the pitching park factor

# join pitching data with the team data on team id, to get the park factor
pitchingWithPpf = pitching.filter(pitching['yearID'] == year).join(teamsPpf.filter(teamsPpf['yearID'] == year), 'teamID')

# column changes: calculate singles, and convert everything to integers
pitchingWithPpf = (pitchingWithPpf
	.withColumn('1B', pitchingWithPpf['H'] - pitchingWithPpf['2B'] - pitchingWithPpf['3B'] - pitchingWithPpf['HR'])
	.withColumn('AB', pitchingWithPpf['AB'].cast(IntegerType()))
	.withColumn('H', pitchingWithPpf['H'].cast(IntegerType()))
	.withColumn('2B', pitchingWithPpf['2B'].cast(IntegerType()))
	.withColumn('3B', pitchingWithPpf['3B'].cast(IntegerType()))
	.withColumn('HR', pitchingWithPpf['HR'].cast(IntegerType()))
	.withColumn('BB', pitchingWithPpf['BB'].cast(IntegerType()))
	.withColumn('IBB', pitchingWithPpf['IBB'].cast(IntegerType()))
	.withColumn('HBP', pitchingWithPpf['HBP'].cast(IntegerType()))
	.withColumn('SF', pitchingWithPpf['SF'].cast(IntegerType()))
	.withColumn('SH', pitchingWithPpf['SH'].cast(IntegerType()))
	.withColumn('GIDP', pitchingWithPpf['GIDP'].cast(IntegerType()))
	.withColumn('SB', pitchingWithPpf['SB'].cast(IntegerType()))
	.withColumn('CS', pitchingWithPpf['CS'].cast(IntegerType()))
	.withColumn('PPF', pitchingWithPpf['PPF'].cast(IntegerType()))
)

# filter minimum at-bats, if specified
if minAB > 0:
	pitchingWithPpf = pitchingWithPpf.filter(pitchingWithPpf['AB'] >= minAB)

# turns this dataframe into an RDD of Row objects
playerRcPpf = (pitchingWithPpf.rdd
	# maps the RDD into <k, v> where k is player ID and v is a tuple (RC, RC27)
	.map(lambda r:
		(
			r['playerID'],
			(round(runsCreated(r['AB'], r['H'], (r['1B']),
				r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
				r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['PPF']), 2),
			round(runsCreated27(r['AB'], r['H'], (r['1B']),
				r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
				r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['PPF']), 2))
		)
	)
	# sum the RC and RC27 by player
	.reduceByKey(rcPairAdder)
	# expand out the RC, RC27 tuple (so it maps easier to a dataframe for export)
	.map(lambda r: (r[0], r[1][0], r[1][1]))
	# sort by RC or RC27
	.sortBy(lambda r: r[sortIndex], ascending=True)
)

output = spark.createDataFrame(playerRcPpf)

if players > 0:
	output = output.limit(players)

output.write.csv("C:\\Users\\Vincent\\pyspark-scripts\\bushong_phase3_" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")
