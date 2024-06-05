package ernestdb

import "time"

const (
	epoch       = "2024-06-04 22:13:57.120656 +0000 UTC"
	epochFormat = "2006-01-02 15:04:05.999999999 -0700 MST"
)

var (
	epochMs int64
)

func init() {
	ts, _ := time.Parse(epochFormat, epoch)
	epochMs = ts.UTC().UnixMilli()
}
