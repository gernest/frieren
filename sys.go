package ernestdb

import (
	"log/slog"
	"os"
)

func exit(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}
