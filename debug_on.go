//go:build debug

package litenetlib

// debugBuild mirrors the reference DEBUG build (-tags debug): statistics (STATS_ENABLED)
// are collected and the network simulation is active.
const debugBuild = true
