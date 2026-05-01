package output

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/fatih/color"
	"github.com/olekukonko/tablewriter"
)

var (
	// Color schemes
	SuccessColor *color.Color
	ErrorColor   *color.Color
	InfoColor    *color.Color
	WarnColor    *color.Color
	KeyColor     *color.Color
	ValueColor   *color.Color
)

func SetupColors() {
	SuccessColor = color.New(color.FgGreen, color.Bold)
	ErrorColor = color.New(color.FgRed, color.Bold)
	InfoColor = color.New(color.FgCyan, color.Bold)
	WarnColor = color.New(color.FgYellow, color.Bold)
	KeyColor = color.New(color.FgBlue, color.Bold)
	ValueColor = color.New(color.FgWhite)
}

func PrintSuccess(format string, args ...interface{}) {
	SuccessColor.Printf(format, args...)
}

func PrintError(format string, args ...interface{}) {
	ErrorColor.Printf(format, args...)
}

func PrintInfo(format string, args ...any) {
	InfoColor.Printf(format, args...)
}

func PrintWarn(format string, args ...interface{}) {
	WarnColor.Printf(format, args...)
}

func PrintKey(key string) {
	KeyColor.Print(key)
}

func PrintValue(value string) {
	ValueColor.Print(value)
}

// TableWriter creates a new table writer
func NewTable() *tablewriter.Table {
	return tablewriter.NewWriter(os.Stdout)
}

func RenderTable(header []string, rows [][]string) {
	table := NewTable()
	table.Header(header)
	for _, row := range rows {
		table.Append(row)
	}
	table.Render()
}

func PrettyPrint(v interface{}) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	enc.Encode(v)
}

func FormatBytes(bytes int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case bytes >= GB:
		return fmt.Sprintf("%.2f GB", float64(bytes)/GB)
	case bytes >= MB:
		return fmt.Sprintf("%.2f MB", float64(bytes)/MB)
	case bytes >= KB:
		return fmt.Sprintf("%.2f KB", float64(bytes)/KB)
	default:
		return fmt.Sprintf("%d B", bytes)
	}
}

func Truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	if maxLen <= 3 {
		return ""
	}
	return s[:maxLen-3] + "..."
}
