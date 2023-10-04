package ae

import (
	"log"

	"golang.org/x/sys/unix"
)

// 文件事件
type FileType int

const (
	FE_READABLE FileType = 1
	FE_WRITABLE FileType = 2
)

var fileEvent2EpollEvent = map[FileType]uint32{
	FE_READABLE: unix.EPOLLIN,  // 可读事件
	FE_WRITABLE: unix.EPOLLOUT, // 可写事件
}

type FileProc func(loop *AeLoop, fd int, extra interface{})

type AeFileEvent struct {
	fd    int
	mask  FileType
	proc  FileProc
	extra interface{}
}

// AddFileEvent 添加文件事件
func (loop *AeLoop) AddFileEvent(fd int, mask FileType, proc FileProc, extra interface{}) {
	ev := loop.getEpollMask(fd)
	if ev&fileEvent2EpollEvent[mask] != 0 {
		return
	}
	op := unix.EPOLL_CTL_ADD

	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	ev |= fileEvent2EpollEvent[mask]
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		log.Printf("epoll ctr err: %v\n", err)
		return
	}
	loop.FileEvents[getFeKey(fd, mask)] = &AeFileEvent{
		fd:    fd,
		mask:  mask,
		proc:  proc,
		extra: extra,
	}
	log.Printf("ae add file event fd:%v, mask:%v\n", fd, mask)
}

// RemoveFileEvent 移出文件事件
func (loop *AeLoop) RemoveFileEvent(fd int, mask FileType) {
	op := unix.EPOLL_CTL_DEL
	ev := loop.getEpollMask(fd)
	ev &= ^fileEvent2EpollEvent[mask]
	if ev != 0 {
		op = unix.EPOLL_CTL_MOD
	}
	err := unix.EpollCtl(loop.fileEventFd, op, fd, &unix.EpollEvent{Fd: int32(fd), Events: ev})
	if err != nil {
		log.Printf("epoll del err: %v\n", err)
	}
	loop.FileEvents[getFeKey(fd, mask)] = nil
	log.Printf("ae remove file event fd:%v, mask:%v\n", fd, mask)
}
