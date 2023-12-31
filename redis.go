package main

import (
	"errors"
	"fmt"
	"go-redis/ae"
	"go-redis/conf"
	"go-redis/http"
	"go-redis/net"
	"go-redis/obj"
	"hash/fnv"
	"log"
	"strconv"
	"strings"
	"time"
)

type CmdType = byte

const (
	COMMAND_UNKNOWN CmdType = 0x00
	COMMAND_INLINE  CmdType = 0x01
	COMMAND_BULK    CmdType = 0x02
)

const (
	IO_BUF     int = 1024 * 16
	MAX_BULK   int = 1024 * 4
	MAX_INLINE int = 1024 * 4
)

var server RedisServer

type RedisServer struct {
	fd      int
	port    int
	db      *redisDB
	clients map[int]*RedisClient
	aeLoop  *ae.AeLoop
}

type redisDB struct {
	data   *obj.Dict
	expire *obj.Dict
}

type RedisClient struct {
	fd       int
	db       *redisDB
	args     []*obj.RedisObj
	reply    *obj.List
	sentLen  int
	queryBuf []byte
	queryLen int
	cmdTy    CmdType
	bulkNum  int
	bulkLen  int
}

func (c *RedisClient) AddReply(o *obj.RedisObj) {
	c.reply.Append(o)
	server.aeLoop.AddFileEvent(c.fd, ae.FE_WRITABLE, SendReplyToClient, c)
}

func (c *RedisClient) AddReplyStr(str string) {
	o := obj.CreateObject(obj.STR, str)
	c.AddReply(o)
}

func ProcessCommand(c *RedisClient) {
	cmdStr := c.args[0].StrVal()
	log.Printf("process command: %v\n", cmdStr)
	if cmdStr == "quit" {
		freeClient(c)
		return
	}
	cmd := lookupCommand(cmdStr)
	if cmd == nil {
		c.AddReplyStr("-ERR: unknow command")
		resetClient(c)
		return
	} else if cmd.arity != len(c.args) {
		c.AddReplyStr("-ERR: wrong number of args")
		resetClient(c)
		return
	}
	cmd.proc(c)
	resetClient(c)
}

func resetClient(client *RedisClient) {
	client.cmdTy = COMMAND_UNKNOWN
	client.bulkLen = 0
	client.bulkNum = 0
}

func (client *RedisClient) findLineInQuery() (int, error) {
	index := strings.Index(string(client.queryBuf[:client.queryLen]), "\n")
	if index < 0 && client.queryLen > MAX_INLINE {
		return index, errors.New("too big inline cmd")
	}
	return index, nil
}

func (client *RedisClient) getNumInQuery(s, e int) (int, error) {
	num, err := strconv.Atoi(string(client.queryBuf[s:e]))
	client.queryBuf = client.queryBuf[e+2:]
	client.queryLen -= e + 2
	return num, err
}

func handleInlineBuf(client *RedisClient) (bool, error) {
	index, err := client.findLineInQuery()
	if index < 0 {
		return false, err
	}

	subs := strings.Split(string(client.queryBuf[:index]), " ")
	client.queryBuf = client.queryBuf[index+1:]
	client.queryLen -= index + 1
	client.args = make([]*obj.RedisObj, len(subs))
	for i, v := range subs {
		client.args[i] = obj.CreateObject(obj.STR, v)
	}

	return true, nil
}

func handleBulkBuf(client *RedisClient) (bool, error) {
	if client.bulkNum == 0 {
		index, err := client.findLineInQuery()
		if index < 0 {
			return false, err
		}

		bnum, err := client.getNumInQuery(1, index)
		if err != nil {
			return false, err
		}
		if bnum == 0 {
			return true, nil
		}
		client.bulkNum = bnum
		client.args = make([]*obj.RedisObj, bnum)
	}
	for client.bulkNum > 0 {
		if client.bulkLen == 0 {
			index, err := client.findLineInQuery()
			if index < 0 {
				return false, err
			}

			if client.queryBuf[0] != '$' {
				return false, errors.New("expect $ for bulk length")
			}

			blen, err := client.getNumInQuery(1, index)
			if err != nil || blen == 0 {
				return false, err
			}
			if blen > MAX_BULK {
				return false, errors.New("too big bulk")
			}
			client.bulkLen = blen
		}
		if client.queryLen < client.bulkLen+2 {
			return false, nil
		}
		index := client.bulkLen
		if client.queryBuf[index] != '\r' || client.queryBuf[index+1] != '\n' {
			return false, errors.New("expect CRLF for bulk end")
		}
		client.args[len(client.args)-client.bulkNum] = obj.CreateObject(obj.STR, string(client.queryBuf[:index]))
		client.queryBuf = client.queryBuf[index+2:]
		client.queryLen -= index + 2
		client.bulkLen = 0
		client.bulkNum -= 1
	}
	return true, nil
}

func ReadQueryFromClient(loop *ae.AeLoop, fd int, extra interface{}) {
	client := extra.(*RedisClient)
	if len(client.queryBuf)-client.queryLen < MAX_BULK {
		client.queryBuf = append(client.queryBuf, make([]byte, MAX_BULK)...)
	}
	n, err := net.Read(fd, client.queryBuf[client.queryLen:])
	if err != nil {
		log.Printf("client %v read err: %v\n", fd, err)
		freeClient(client)
		return
	}
	if n == 0 {
		log.Printf("process query buf empty")
		freeClient(client)
		return
	}
	client.queryLen += n
	log.Printf("read %v bytes from client:%v\n", n, client.fd)
	log.Printf("ReadQueryFromClient, queryBuf : %v\n", string(client.queryBuf))
	err = ProcessQueryBuf(client)
	if err != nil {
		log.Printf("process query buf err: %v\n", err)
		freeClient(client)
		return
	}
}

func ProcessQueryBuf(client *RedisClient) error {
	for client.queryLen > 0 {
		if client.cmdTy == COMMAND_UNKNOWN {
			if client.queryBuf[0] == '*' {
				client.cmdTy = COMMAND_BULK
			} else {
				client.cmdTy = COMMAND_INLINE
			}
		}
		var ok bool
		var err error
		if client.cmdTy == COMMAND_INLINE {
			ok, err = handleInlineBuf(client)
		} else if client.cmdTy == COMMAND_BULK {
			ok, err = handleBulkBuf(client)
		} else {
			return errors.New("unknow Redis Command Type")
		}
		if err != nil {
			return err
		}
		if ok {
			if len(client.args) == 0 {
				resetClient(client)
			} else {
				ProcessCommand(client)
			}
		} else {
			break
		}
	}
	return nil
}

type CommandProc func(c *RedisClient)

type RedisCommand struct {
	name  string
	proc  CommandProc
	arity int
}

var cmdTable []RedisCommand = []RedisCommand{
	{"get", getCommand, 2},
	{"set", setCommand, 3},
	{"expire", expireCommand, 3},
}

func expireIfNeeded(key *obj.RedisObj) {
	entry := server.db.expire.Find(key)
	if entry == nil {
		return
	}
	when := entry.Val.IntVal()
	if when > ae.GetMsTime() {
		return
	}
	server.db.expire.Delete(key)
	server.db.data.Delete(key)
}

func findKeyRead(key *obj.RedisObj) *obj.RedisObj {
	expireIfNeeded(key)
	return server.db.data.Get(key)
}

func getCommand(c *RedisClient) {
	key := c.args[1]
	val := findKeyRead(key)
	if val == nil {
		c.AddReplyStr("$-1\r\n")
	} else if val.Type != obj.STR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	} else {
		str := val.StrVal()
		c.AddReplyStr(fmt.Sprintf("$%d%v\r\n", len(str), str))
	}
}

func setCommand(c *RedisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type != obj.STR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	server.db.data.Set(key, val)
	server.db.expire.Delete(key)
	c.AddReplyStr("+OK\r\n")
}

func expireCommand(c *RedisClient) {
	key := c.args[1]
	val := c.args[2]
	if val.Type != obj.STR {
		c.AddReplyStr("-ERR: wrong type\r\n")
	}
	expire := ae.GetMsTime() + (val.IntVal() * 1000)
	expObj := obj.CreateFromInt(expire)
	server.db.expire.Set(key, expObj)
	c.AddReplyStr("+OK\r\n")
}

func lookupCommand(cmdStr string) *RedisCommand {
	for _, c := range cmdTable {
		if c.name == cmdStr {
			return &c
		}
	}
	return nil
}

func freeReplyList(client *RedisClient) {
	for client.reply.Length != 0 {
		n := client.reply.Head
		client.reply.DelNode(n)
	}
}

func freeClient(client *RedisClient) {
	delete(server.clients, client.fd)
	server.aeLoop.RemoveFileEvent(client.fd, ae.FE_READABLE)
	server.aeLoop.RemoveFileEvent(client.fd, ae.FE_WRITABLE)
	freeReplyList(client)
	net.Close(client.fd)
	log.Printf("close client fd:%d\n", client.fd)
}

func SendReplyToClient(loop *ae.AeLoop, fd int, extra interface{}) {
	client := extra.(*RedisClient)
	log.Printf("SendReplyToClient, reply len:%v\n", client.reply.Length)
	for client.reply.Length > 0 {
		rep := client.reply.Head
		buf := []byte(rep.Val.StrVal())
		bufLen := len(buf)
		if client.sentLen < bufLen {
			n, err := net.Write(fd, buf[client.sentLen:])
			if err != nil {
				log.Printf("send reply err: %v\n", err)
				freeClient(client)
				return
			}
			client.sentLen += n
			log.Printf("send %v bytes to client:%v\n", n, client.fd)
			if client.sentLen == bufLen {
				client.reply.DelNode(rep)
				client.sentLen = 0
			} else {
				break
			}
		}
	}
	if client.reply.Length == 0 {
		client.sentLen = 0
		loop.RemoveFileEvent(fd, ae.FE_WRITABLE)
	}
}

func GStrEqual(a, b *obj.RedisObj) bool {
	if a.Type != obj.STR || b.Type != obj.STR {
		return false
	}
	return a.StrVal() == b.StrVal()
}

func GStrHash(key *obj.RedisObj) int64 {
	if key.Type != obj.STR {
		return 0
	}
	hash := fnv.New64()
	hash.Write([]byte(key.StrVal()))
	return int64(hash.Sum64())
}

func CreateClient(fd int) *RedisClient {
	var client RedisClient
	client.fd = fd
	client.db = server.db
	client.queryBuf = make([]byte, IO_BUF)
	client.reply = obj.ListCreate(obj.ListType{EqualFunc: GStrEqual})
	return &client
}

func AcceptHandler(loop *ae.AeLoop, fd int, extra interface{}) {
	cfd, err := net.Accept(fd)
	if err != nil {
		log.Printf("accept err: %v\n", err)
		return
	}
	client := CreateClient(cfd)
	server.clients[cfd] = client
	server.aeLoop.AddFileEvent(cfd, ae.FE_READABLE, ReadQueryFromClient, client)
	log.Printf("accept client, fd: %v\n", cfd)
}

const EXPIRE_CHECK_COUNT int = 100

func ServerCron(loop *ae.AeLoop, id int, extra interface{}) {
	for i := 0; i < EXPIRE_CHECK_COUNT; i++ {
		entry := server.db.expire.RandomGet()
		if entry == nil {
			break
		}
		if entry.Val.IntVal() < time.Now().Unix() {
			server.db.data.Delete(entry.Key)
			server.db.expire.Delete(entry.Key)
		}
	}
}

func initServer(config *conf.Config) error {
	server.port = config.Port
	server.clients = make(map[int]*RedisClient)
	server.db = &redisDB{
		data:   obj.DictCreate(obj.DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
		expire: obj.DictCreate(obj.DictType{HashFunc: GStrHash, EqualFunc: GStrEqual}),
	}
	var err error
	server.fd, err = net.TcpServer(server.port)
	if err != nil {
		return err
	}
	server.aeLoop, err = ae.AeLoopCreate(server.fd)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)
	config, err := conf.LoadConfig()
	if err != nil {
		log.Fatalf("config error: %v\n", err)
	}
	err = initServer(config)
	if err != nil {
		log.Fatalf("init server error: %v\n", err)
	}
	server.aeLoop.AddFileEvent(server.fd, ae.FE_READABLE, AcceptHandler, nil)
	server.aeLoop.AddTimeEvent(ae.TE_NORMAL, 100, ServerCron, nil)
	log.Println("redis server is up.")

	if config.HttpAddr != "" {
		http.StartHttpListen(config.HttpAddr).
			AddRoute("/key/get", getCommandHttp).
			AddRoute("/key/set", setCommandHttp).
			AddRoute("/key/expire", expireCommandHttp)
	}

	server.aeLoop.AeMain()
}

func getCommandHttp(arg ...string) string {
	if len(arg) != 1 {
		return "-1"
	}
	valObj := findKeyRead(&obj.RedisObj{Val: arg[0]})
	if valObj == nil {
		return "-1"
	} else if valObj.Type != obj.STR {
		return "wrong type"
	} else {
		return valObj.StrVal()
	}
}

func setCommandHttp(arg ...string) string {
	if len(arg) != 2 {
		return "-1"
	}
	keyObj := &obj.RedisObj{Val: arg[0]}
	valObj := &obj.RedisObj{Val: arg[1]}
	server.db.data.Set(keyObj, valObj)
	return "OK"
}

func expireCommandHttp(arg ...string) string {
	if len(arg) != 2 {
		return "-1"
	}
	keyObj := &obj.RedisObj{Val: arg[0]}
	valObj := findKeyRead(&obj.RedisObj{Val: arg[0]})
	if valObj == nil {
		return "-1"
	}

	expire, err := strconv.Atoi(arg[1])
	if err != nil {
		return "-1"
	}
	e := ae.GetMsTime() + int64(expire*1000)
	expObj := obj.CreateFromInt(e)
	server.db.expire.Set(keyObj, expObj)
	return "OK"
}
