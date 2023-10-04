package ae

import (
	"log"

	"golang.org/x/sys/unix"
)

// 事件循环
type AeLoop struct {
	FileEvents      map[int]*AeFileEvent // 文件事件
	TimeEvents      *AeTimeEvent         // 时间事件
	fileEventFd     int
	timeEventNextId int
	stop            bool
}

func getFeKey(fd int, mask FileType) int {
	if mask == FE_READABLE {
		return fd
	} else {
		return fd * -1
	}
}

func (loop *AeLoop) getEpollMask(fd int) uint32 {
	var ev uint32
	if loop.FileEvents[getFeKey(fd, FE_READABLE)] != nil {
		ev |= fileEvent2EpollEvent[FE_READABLE]
	}
	if loop.FileEvents[getFeKey(fd, FE_WRITABLE)] != nil {
		ev |= fileEvent2EpollEvent[FE_WRITABLE]
	}
	return ev
}

// AeLoopCreate 创建
func AeLoopCreate() (*AeLoop, error) {
	epollFd, err := unix.Kqueue()
	if err != nil {
		return nil, err
	}
	return &AeLoop{
		FileEvents:      make(map[int]*AeFileEvent),
		fileEventFd:     epollFd,
		timeEventNextId: 1,
		stop:            false,
	}, nil
}

// nearestTime 最近的时间事件到达点
func (loop *AeLoop) nearestTime() int64 {
	var nearest int64 = GetMsTime() + 1000
	p := loop.TimeEvents
	for p != nil {
		if p.when < nearest {
			nearest = p.when
		}
		p = p.next
	}
	return nearest
}

// AeWait 等待下一个 文件事件或时间事件
func (loop *AeLoop) AeWait() (tes []*AeTimeEvent, fes []*AeFileEvent) {
	timeout := loop.nearestTime() - GetMsTime()
	if timeout <= 0 {
		timeout = 10
	}
	var events [128]unix.EpollEvent
	n, err := unix.EpollWait(loop.fileEventFd, events[:], int(timeout)) // fe不能无限等待，会导致te事件阻塞
	if err != nil {
		log.Printf("epoll wait warnning: %v\n", err)
	}
	if n > 0 {
		log.Printf("ae get %v epoll events\n", n)
	}
	for i := 0; i < n; i++ {
		if events[i].Events&unix.EPOLLIN != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), FE_READABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
		if events[i].Events&unix.EPOLLOUT != 0 {
			fe := loop.FileEvents[getFeKey(int(events[i].Fd), FE_WRITABLE)]
			if fe != nil {
				fes = append(fes, fe)
			}
		}
	}
	now := GetMsTime()
	p := loop.TimeEvents
	for p != nil {
		if p.when <= now {
			tes = append(tes, p)
		}
		p = p.next
	}
	return
}

// AeProcess 事件处理
func (loop *AeLoop) AeProcess(tes []*AeTimeEvent, fes []*AeFileEvent) {
	for _, te := range tes {
		te.proc(loop, te.id, te.extra)
		if te.mask == TE_ONCE {
			loop.RemoveTimeEvent(te.id)
		} else {
			te.when = GetMsTime() + te.interval
		}
	}
	if len(fes) > 0 {
		log.Println("ae is processing file events")
		for _, fe := range fes {
			fe.proc(loop, fe.fd, fe.extra)
		}
	}
}

// AeMain 主循环
func (loop *AeLoop) AeMain() {
	for loop.stop != true {
		tes, fes := loop.AeWait()
		loop.AeProcess(tes, fes)
	}
}
