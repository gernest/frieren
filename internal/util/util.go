package util

import (
	"log/slog"
	"os"
	"time"
)

func Exit(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func TS() time.Time {
	ts, _ := time.Parse(time.RFC822, time.RFC822)
	return ts.UTC()
}
