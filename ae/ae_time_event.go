package ae

import (
	"time"
)

// 时间事件
type TimeType int

const (
	TE_NORMAL TimeType = 1
	TE_ONCE   TimeType = 2
)

type TimeProc func(loop *AeLoop, id int, extra interface{})

type AeTimeEvent struct {
	id       int
	mask     TimeType
	when     int64 // 时间点 ms
	interval int64 // 时间间隔 ms
	proc     TimeProc
	extra    interface{}
	next     *AeTimeEvent
}

// GetMsTime 当前时间ms
func GetMsTime() int64 {
	return time.Now().UnixNano() / 1e6
}

// AddTimeEvent 添加时间事件
func (loop *AeLoop) AddTimeEvent(mask TimeType, interval int64, proc TimeProc, extra interface{}) int {
	id := loop.timeEventNextId
	loop.timeEventNextId++
	var te AeTimeEvent
	te.id = id
	te.mask = mask
	te.interval = interval
	te.when = GetMsTime() + interval
	te.proc = proc
	te.extra = extra
	te.next = loop.TimeEvents
	loop.TimeEvents = &te
	return id
}

// RemoveTimeEvent 移出时间事件
func (loop *AeLoop) RemoveTimeEvent(id int) {
	p := loop.TimeEvents
	var pre *AeTimeEvent
	for p != nil {
		if p.id == id {
			if pre == nil {
				loop.TimeEvents = p.next
			} else {
				pre.next = p.next
			}
			p.next = nil
			break
		}
		pre = p
		p = p.next
	}
}
