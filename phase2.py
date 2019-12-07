from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from datetime import datetime
import argparse
import sys

def runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
  timesOnBase = H + BB - CS + HBP - GIDP
  #basesAdvanced = .82 * (B1 + 2*B2 + 3*B3 + 4*HR) + .37*(BB - IBB + HBP) + .45*(SH + SF + SB) 
  #basesAdvanced = .93 * (B1 + 2*B2 + 3*B3 + 4*HR) + .24*(BB - IBB + HBP) + .57*(SH + SF + SB)
  #basesAdvanced = 0.904886081472875 * (B1 + 2*B2 + 3*B3 + 4*HR) + 0.09235178878271577 * (BB - IBB + HBP) + 0.5751517433371399 * (SH + SF + SB) # w/ runs park adjusted
  #basesAdvanced = .888 * (B1 + 2*B2 + 3*B3 + 4*HR) + .206*(BB - IBB + HBP) + .547*(SH + SF + SB) # just for fun, from 2016
  #basesAdvanced = .908 * (B1 + 2*B2 + 3*B3 + 4*HR) + .095*(BB - IBB + HBP) + .592*(SH + SF + SB) # w/ no regparam or elasticnetparam
  basesAdvanced = (0.9987044787802928 * (B1 + 2*B2 + 3*B3 + 4*HR) +
                   0.21029122846958778 *(BB - IBB + HBP) +
                   0.39392640459716954 * (SH + SF + SB))
  opportunities = AB + BB + HBP + SF + SH
  if opportunities == 0:
    return 0
  return ((timesOnBase * basesAdvanced) / opportunities) / (((BPF / 100) + 1) / 2)

def runsCreated27(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
  outs = AB - H + SF + SH + GIDP + CS
  if outs == 0:
    return 0
  return (runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF) * 27) / outs

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

print(year, minAB, sort, players)
print(sortIndex)

battingFile = "C:\\Users\\Vincent\\Downloads\\baseballdatabank-2019.2\\baseballdatabank-2019.2\\core\\Batting.csv"
#battingFile = "C:\\Users\\Vincent\\pyspark-scripts\\Batting_alt_BB.csv"
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

# filter minimum at-bats, if specified
if minAB > 0:
  battingWithBpf = battingWithBpf.filter(battingWithBpf['AB'] >= minAB)

playerRcBpf = (battingWithBpf.rdd
  .map(lambda r:
    (
      r['playerID'],
      round(runsCreated(r['AB'], r['H'], (r['1B']),
        r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
        r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['BPF']), 2),
      round(runsCreated27(r['AB'], r['H'], (r['1B']),
        r['2B'], r['3B'], r['HR'], r['BB'], r['IBB'], r['HBP'],
        r['SF'], r['SH'], r['GIDP'], r['SB'], r['CS'], r['BPF']), 2)
    )
  )
  .sortBy(lambda r: r[sortIndex], ascending=False)
)

print(playerRcBpf)

output = spark.createDataFrame(playerRcBpf)

if players > 0:
  output = output.limit(players)

#output = output.map(lambda r: ','.join([r[0], str(r[1]), str(r[2])]))
output.write.csv("C:\\Users\\Vincent\\pyspark-scripts\\phase2" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")
#output.saveAsTextFile("C:\\Users\\Vincent\\pyspark-scripts\\test" + datetime.now().strftime("%Y-%m-%d_%H%M%S") + ".csv")
