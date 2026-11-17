// Package format renders sizes and rates for logs and the CLI.
package format

import "fmt"

// Bytes renders a byte count in a compact binary unit.
func Bytes(n int64) string {
	if n < 0 {
		return fmt.Sprintf("%d B", n)
	}
	units := []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}
	value := float64(n)
	unit := units[0]
	for i := 0; i < len(units)-1 && value >= 1024; i++ {
		value /= 1024
		unit = units[i+1]
	}
	if unit == "B" {
		return fmt.Sprintf("%d B", n)
	}
	if value >= 100 || value == float64(int64(value)) {
		return fmt.Sprintf("%.0f %s", value, unit)
	}
	return fmt.Sprintf("%.1f %s", value, unit)
}

// Rate renders a per-second throughput.
func Rate(bytesPerSecond float64) string {
	if bytesPerSecond < 0 {
		bytesPerSecond = 0
	}
	return Bytes(int64(bytesPerSecond)) + "/s"
}
