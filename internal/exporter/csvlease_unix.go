//go:build aix || darwin || dragonfly || freebsd || illumos || linux || netbsd || openbsd || solaris

package exporter

func csvGenerationPruningSupported() bool { return true }
