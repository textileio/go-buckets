package cmd

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"

	logging "github.com/ipfs/go-log/v2"
	aurora2 "github.com/logrusorgru/aurora"
	"github.com/olekukonko/tablewriter"
	"google.golang.org/grpc/status"
)

var aurora = aurora2.NewAurora(runtime.GOOS != "windows")

func Message(format string, args ...interface{}) {
	if format == "" {
		return
	}
	fmt.Println(aurora.Sprintf(aurora.BrightBlack("> "+format), args...))
}

func Success(format string, args ...interface{}) {
	fmt.Println(aurora.Sprintf(aurora.Cyan("> Success! %s"),
		aurora.Sprintf(aurora.BrightBlack(format), args...)))
}

func Warn(format string, args ...interface{}) {
	if format == "" {
		return
	}
	fmt.Println(aurora.Sprintf(aurora.Yellow("> Warning! %s"),
		aurora.Sprintf(aurora.BrightBlack(format), args...)))
}

func Err(err error, args ...interface{}) {
	var msg string
	stat, ok := status.FromError(err)
	if ok {
		msg = stat.Message()
	} else {
		msg = err.Error()
	}
	words := strings.SplitN(msg, " ", 2)
	words[0] = strings.Title(words[0])
	msg = strings.Join(words, " ")

	fmt.Println(aurora.Sprintf(aurora.Red("> Error! %s"),
		aurora.Sprintf(aurora.BrightBlack(msg), args...)))
}

func End(format string, args ...interface{}) {
	Message(format, args...)
	os.Exit(0)
}

func Fatal(err error, args ...interface{}) {
	Err(err, args...)
	os.Exit(1)
}

func ErrCheck(err error, args ...interface{}) {
	if err != nil {
		Fatal(err, args...)
	}
}

func LogErr(err error, args ...interface{}) {
	if err != nil {
		Err(err, args...)
	}
}

func RenderJSON(data interface{}) {
	bytes, err := json.MarshalIndent(data, "", "  ")
	ErrCheck(err)
	fmt.Println(string(bytes))
}

func RenderTable(header []string, data [][]string) {
	fmt.Println()
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader(header)
	table.SetAutoWrapText(false)
	table.SetAutoFormatHeaders(true)
	table.SetHeaderAlignment(tablewriter.ALIGN_LEFT)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetCenterSeparator("")
	table.SetColumnSeparator("")
	table.SetRowSeparator("")
	table.SetHeaderLine(false)
	table.SetBorder(false)
	table.SetTablePadding("\t")
	table.SetNoWhiteSpace(false)
	headersColors := make([]tablewriter.Colors, len(header))
	for i := range headersColors {
		headersColors[i] = tablewriter.Colors{tablewriter.FgHiBlackColor}
	}
	table.SetHeaderColor(headersColors...)
	table.AppendBulk(data)
	table.Render()
	fmt.Println()
}

func HandleInterrupt(stop func()) {
	quit := make(chan os.Signal)
	signal.Notify(quit, os.Interrupt)
	<-quit
	fmt.Println("Gracefully stopping... (press Ctrl+C again to force)")
	stop()
	os.Exit(1)
}

func SetupDefaultLoggingConfig(file string) error {
	if file != "" {
		if err := os.MkdirAll(filepath.Dir(file), os.ModePerm); err != nil {
			return err
		}
	}
	c := logging.Config{
		Format: logging.ColorizedOutput,
		Stderr: true,
		File:   file,
		Level:  logging.LevelError,
	}
	logging.SetupLogging(c)
	return nil
}
