package obj

import "strconv"

type RedisType uint8

const (
	STR  RedisType = 0x00
	LIST RedisType = 0x01
	DICT RedisType = 0x02
)

type RedisVal interface{}

type RedisObj struct {
	Type RedisType
	Val  RedisVal
}

func (o *RedisObj) IntVal() int64 {
	if o.Type != STR {
		return 0
	}
	val, _ := strconv.ParseInt(o.Val.(string), 10, 64)
	return val
}

func (o *RedisObj) StrVal() string {
	if o.Type != STR {
		return ""
	}
	return o.Val.(string)
}

func CreateFromInt(val int64) *RedisObj {
	return &RedisObj{
		Type: STR,
		Val:  strconv.FormatInt(val, 10),
	}
}

func CreateObject(typ RedisType, ptr interface{}) *RedisObj {
	return &RedisObj{
		Type: typ,
		Val:  ptr,
	}
}
