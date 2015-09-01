//go:generate protoc --go_out=. common/lines.proto

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/badoo/thunder/logscol"
	"github.com/badoo/thunder/logsproc"
)

const (
	ACTION_LOGS_COLLECTOR = "logs-collector"
	ACTION_LOGS_PROCESSOR = "logs-processor"
)

func dieWithUsage() {
	fmt.Fprintln(os.Stderr, "Usage: thunder <action> [<flags>]")
	fmt.Fprintln(os.Stderr, "  <action> can be one of the following:")
	fmt.Fprintln(os.Stderr, "     "+ACTION_LOGS_COLLECTOR+" - logs collector")
	fmt.Fprintln(os.Stderr, "     "+ACTION_LOGS_PROCESSOR+" - logs processor")
	os.Exit(1)
}

func main() {
	var action string

	// rewrite arguments so that we can support multiple "actions" in a single binary
	newArgs := make([]string, 0, len(os.Args))
	for i, arg := range os.Args {
		if i > 0 && arg[0] != '-' && action == "" {
			action = arg
			continue
		}

		newArgs = append(newArgs, arg)
	}
	os.Args = newArgs

	if action == "" {
		fmt.Fprintln(os.Stderr, "Error: action was not specified")
		dieWithUsage()
	}

	if action == ACTION_LOGS_COLLECTOR {
		logscol.InitFlags()
		flag.Parse()
		logscol.Run()
		return
	} else if action == ACTION_LOGS_PROCESSOR {
		logsproc.InitFlags()
		flag.Parse()
		logsproc.Run()
		return
	}

	dieWithUsage()
}
