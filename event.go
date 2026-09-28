package streamengine

import (
	"strconv"
	"time"
)

// Event 是业务事件，携带事件时间（event time）。
type Event struct {
	Key       string
	EventTime time.Time
	Value     int64
}

// Record 是数据源投递的一条记录，Partition+Offset 唯一标识消费位置。
type Record struct {
	Partition int
	Offset    int64
	Event     Event
}

// positionKey 是去重与位点对齐使用的唯一键。
func positionKey(partition int, offset int64) string {
	return strconv.Itoa(partition) + ":" + strconv.FormatInt(offset, 10)
}
