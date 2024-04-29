package log

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func NewLogger(module string) zerolog.Logger {
	zerolog.TimeFieldFormat = time.RFC3339Nano
	output := zerolog.ConsoleWriter{Out: os.Stdout, TimeFormat: "15:04:05.000"}
	output.FormatMessage = func(i interface{}) string {
		return fmt.Sprintf("%-45s", fmt.Sprintf("[%s] %s", strings.ToUpper(module), i))
	}

	return log.Output(output)
}
