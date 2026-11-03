//go:build !debug

package litenetlib

// debugBuild mirrors the reference Release build: most statistics are not collected
// and the network simulation is inactive. Build with -tags debug to enable both.
const debugBuild = false
