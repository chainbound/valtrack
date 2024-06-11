package main

import (
	"log"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/chainbound/valtrack/cmd"
)

func main() {
	app := &cli.App{
		Name:  "valtrack",
		Usage: "Ethereum consensus validator tracking tool",
		Commands: []*cli.Command{
			cmd.SentryCommand,
			cmd.ConsumerCommand,
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
