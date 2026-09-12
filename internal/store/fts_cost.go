package store

// FTSDerivedCost is the persistent ledger cost, shared by live updates and recovery.
func FTSDerivedCost(text string, tokens []string) (work, bytes uint64) {
	work = addSaturated(multiplySaturated(uint64(len(text)), 3), uint64(len(tokens)))
	for _, token := range tokens {
		bytes = addSaturated(bytes, uint64(len(token))+144)
	}
	return work, bytes
}
