// Package buildinfo carries version metadata set at link time.
package buildinfo

import "runtime/debug"

// Set with -ldflags "-X github.com/aurimasniekis/go-pulse-github/internal/buildinfo.Version=…".
var (
	Version = "dev"
	Commit  = ""
	Date    = ""
)

// PulseSDKVersion is the linked Pulse SDK module version.
func PulseSDKVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, d := range bi.Deps {
		if d.Path == "github.com/aurimasniekis/go-pulse" {
			return d.Version
		}
	}
	return ""
}
