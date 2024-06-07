package util

import (
	"log/slog"
	"os"
	"unsafe"
)

func Uint64ToBytes(in []uint64) []byte {
	return reinterpretSlice[byte](in)
}

func reinterpretSlice[Out, T any](b []T) []Out {
	if cap(b) == 0 {
		return nil
	}
	out := (*Out)(unsafe.Pointer(&b[:1][0]))

	lenBytes := len(b) * int(unsafe.Sizeof(b[0]))
	capBytes := cap(b) * int(unsafe.Sizeof(b[0]))

	lenOut := lenBytes / int(unsafe.Sizeof(*out))
	capOut := capBytes / int(unsafe.Sizeof(*out))

	return unsafe.Slice(out, capOut)[:lenOut]
}

func Exit(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}
