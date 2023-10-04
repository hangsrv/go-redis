package obj

import (
	"errors"
	"math"
	"math/rand"
)

const (
	INIT_SIZE    int64 = 8 // 初始化table大小
	FORCE_RATIO  int64 = 2 // 扩容比率
	GROW_RATIO   int64 = 2 // 扩容倍率
	DEFAULT_STEP int   = 1 // 扩容步数
)

var (
	ErrExpand   = errors.New("expand error")
	ErrExist    = errors.New("key exists error")
	ErrNotExist = errors.New("key doesnt exist error")
)

type Entry struct {
	Key  *RedisObj
	Val  *RedisObj
	next *Entry
}

type htable struct {
	table []*Entry
	size  int64
	mask  int64
	used  int64
}

type DictType struct {
	HashFunc  func(key *RedisObj) int64
	EqualFunc func(k1, k2 *RedisObj) bool
}

type Dict struct {
	DictType
	hts       [2]*htable
	rehashidx int64
}

func DictCreate(dictType DictType) *Dict {
	var dict Dict
	dict.DictType = dictType
	dict.rehashidx = -1
	return &dict
}

func (dict *Dict) isRehashing() bool {
	return dict.rehashidx != -1
}

func (dict *Dict) rehashStep() {
	dict.rehash(DEFAULT_STEP)
}

func (dict *Dict) rehash(step int) {
	for step > 0 {
		if dict.hts[0].used == 0 {
			dict.hts[0] = dict.hts[1]
			dict.hts[1] = nil
			dict.rehashidx = -1
			return
		}
		for dict.hts[0].table[dict.rehashidx] == nil {
			dict.rehashidx += 1
		}
		entry := dict.hts[0].table[dict.rehashidx]
		for entry != nil {
			ne := entry.next
			idx := dict.HashFunc(entry.Key) & dict.hts[1].mask
			entry.next = dict.hts[1].table[idx]
			dict.hts[1].table[idx] = entry
			dict.hts[0].used -= 1
			dict.hts[1].used += 1
			entry = ne
		}
		dict.hts[0].table[dict.rehashidx] = nil
		dict.rehashidx += 1
		step -= 1
	}
}

func nextPower(size int64) int64 {
	for i := INIT_SIZE; i < math.MaxInt64; i *= 2 {
		if i >= size {
			return i
		}
	}
	return -1
}

func (dict *Dict) expand(size int64) error {
	sz := nextPower(size)
	if dict.isRehashing() || (dict.hts[0] != nil && dict.hts[0].size >= sz) {
		return ErrExpand
	}
	var ht htable
	ht.size = sz
	ht.mask = sz - 1
	ht.table = make([]*Entry, sz)
	ht.used = 0
	if dict.hts[0] == nil {
		dict.hts[0] = &ht
		return nil
	}
	dict.hts[1] = &ht
	dict.rehashidx = 0
	return nil
}

func (dict *Dict) expandIfNeeded() error {
	if dict.isRehashing() {
		return nil
	}
	if dict.hts[0] == nil {
		return dict.expand(INIT_SIZE)
	}
	if (dict.hts[0].used > dict.hts[0].size) && (dict.hts[0].used/dict.hts[0].size > FORCE_RATIO) {
		return dict.expand(dict.hts[0].size * GROW_RATIO)
	}
	return nil
}

func (dict *Dict) keyIndex(key *RedisObj) int64 {
	err := dict.expandIfNeeded()
	if err != nil {
		return -1
	}
	h := dict.HashFunc(key)
	var idx int64
	for i := 0; i <= 1; i++ {
		idx = h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return -1
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return idx
}

func (dict *Dict) addRaw(key *RedisObj) *Entry {
	if dict.isRehashing() {
		dict.rehashStep()
	}
	idx := dict.keyIndex(key)
	if idx == -1 {
		return nil
	}
	var ht *htable
	if dict.isRehashing() {
		ht = dict.hts[1]
	} else {
		ht = dict.hts[0]
	}
	var e Entry
	e.Key = key
	e.next = ht.table[idx]
	ht.table[idx] = &e
	ht.used += 1
	return &e
}

func (dict *Dict) add(key, val *RedisObj) error {
	entry := dict.addRaw(key)
	if entry == nil {
		return ErrExist
	}
	entry.Val = val
	return nil
}

func (dict *Dict) Set(key, val *RedisObj) {
	if err := dict.add(key, val); err == nil {
		return
	}
	entry := dict.Find(key)
	entry.Val = val
}

func (dict *Dict) Delete(key *RedisObj) error {
	if dict.hts[0] == nil {
		return ErrNotExist
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		var prev *Entry
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				if prev == nil {
					dict.hts[i].table[idx] = e.next
				} else {
					prev.next = e.next
				}
				return nil
			}
			prev = e
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return ErrNotExist
}

func (dict *Dict) Find(key *RedisObj) *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	if dict.isRehashing() {
		dict.rehashStep()
	}
	h := dict.HashFunc(key)
	for i := 0; i <= 1; i++ {
		idx := h & dict.hts[i].mask
		e := dict.hts[i].table[idx]
		for e != nil {
			if dict.EqualFunc(e.Key, key) {
				return e
			}
			e = e.next
		}
		if !dict.isRehashing() {
			break
		}
	}
	return nil
}

func (dict *Dict) Get(key *RedisObj) *RedisObj {
	entry := dict.Find(key)
	if entry == nil {
		return nil
	}
	return entry.Val
}

func (dict *Dict) RandomGet() *Entry {
	if dict.hts[0] == nil {
		return nil
	}
	t := 0
	if dict.isRehashing() {
		dict.rehashStep()
		if dict.hts[1] != nil && dict.hts[1].used > dict.hts[0].used {
			t = 1
		}
	}
	idx := rand.Int63n(dict.hts[t].size)
	cnt := 0
	for dict.hts[t].table[idx] == nil && cnt < 1000 {
		idx = rand.Int63n(dict.hts[t].size)
		cnt += 1
	}
	if dict.hts[t].table[idx] == nil {
		return nil
	}
	var listLen int64
	p := dict.hts[t].table[idx]
	for p != nil {
		listLen += 1
		p = p.next
	}
	listIdx := rand.Int63n(listLen)
	p = dict.hts[t].table[idx]
	for i := int64(0); i < listIdx; i++ {
		p = p.next
	}
	return p
}
