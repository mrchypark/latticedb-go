//go:build js || plan9 || wasip1 || windows

package exporter

func csvGenerationPruningSupported() bool { return false }
