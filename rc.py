def runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
	timesOnBase = H + BB - CS + HBP - GIDP
	basesAdvanced = (B1 + 2*B2 + 3*B3 + 4*HR) + .26*(BB - IBB + HBP) + .52*(SH + SF + SB)
	opportunities = AB + BB + HBP + SF + SH
	if opportunities == 0:
		return 0
	return ((timesOnBase * basesAdvanced) / opportunities)# / (BPF / 100)

def runsCreated27(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF):
	outs = AB - H + SF + SH + GIDP + CS
	if outs == 0:
		return 0
	return (runsCreated(AB, H, B1, B2, B3, HR, BB, IBB, HBP, SF, SH, GIDP, SB, CS, BPF) * 27) / outs

print(runsCreated(165567, 42215, 26954, 8397, 795, 6105, 15829, 970, 1763, 1168, 925, 3804, 2527, 934, 100))
print(runsCreated27(165567, 42215, 26954, 8397, 795, 6105, 15829, 970, 1763, 1168, 925, 3804, 2527, 934, 100))