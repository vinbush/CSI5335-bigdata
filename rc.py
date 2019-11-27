def runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
	timesOnBase = H + BB - CS + HBP - GIDP
	basesAdvanced = (B1 + 2*B2 + 3*B3 + 4*HR) + .26*(BB - IBB + HBP) + .52*(SH + SF + SB)
	opportunities = AB + BB + HBP + SF + SH

	# For regression stuff
	totalBases = (B1 + 2*B2 + 3*B3 + 4*HR)
	adjustedWalks = BB - IBB + HBP
	sacrificesSteals = SH + SF + SB

	print('B: ', totalBases * timesOnBase / opportunities)
	print('C: ', adjustedWalks * timesOnBase / opportunities)
	print('D: ', sacrificesSteals * timesOnBase / opportunities)
	
	if opportunities == 0:
		return 0
	return ((timesOnBase * basesAdvanced) / opportunities)# / (BPF / 100)

def runsCreated27(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
	outs = AB - H + SF + SH + GIDP + CS
	if outs == 0:
		return 0
	return (runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF) * 27) / outs

print(runsCreated(5511, 1449, 875, 333, 29, 212, 604, 30, 50, 45, 23, 125, 88, 23, 4346))
