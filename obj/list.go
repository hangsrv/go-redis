package obj

type Node struct {
	Val  *RedisObj
	next *Node
	prev *Node
}

type ListType struct {
	EqualFunc func(a, b *RedisObj) bool
}

type List struct {
	ListType
	Head   *Node
	Tail   *Node
	Length int
}

func ListCreate(listType ListType) *List {
	var list List
	list.ListType = listType
	return &list
}

func (list *List) Find(val *RedisObj) *Node {
	p := list.Head
	for p != nil {
		if list.EqualFunc(p.Val, val) {
			break
		}
		p = p.next
	}
	return p
}

func (list *List) Append(val *RedisObj) {
	var n Node
	n.Val = val
	if list.Head == nil {
		list.Head = &n
		list.Tail = &n
	} else {
		n.prev = list.Tail
		list.Tail.next = &n
		list.Tail = list.Tail.next
	}
	list.Length += 1
}

func (list *List) LPush(val *RedisObj) {
	var n Node
	n.Val = val
	if list.Head == nil {
		list.Head = &n
		list.Tail = &n
	} else {
		n.next = list.Head
		list.Head.prev = &n
		list.Head = &n
	}
	list.Length += 1
}

func (list *List) DelNode(n *Node) {
	if n == nil {
		return
	}
	if list.Head == n {
		if n.next != nil {
			n.next.prev = nil
		}
		list.Head = n.next
		n.next = nil
	} else if list.Tail == n {
		if n.prev != nil {
			n.prev.next = nil
		}
		list.Tail = n.prev
		n.prev = nil
	} else {
		if n.prev != nil {
			n.prev.next = n.next
		}
		if n.next != nil {
			n.next.prev = n.prev
		}
		n.prev = nil
		n.next = nil
	}
	list.Length -= 1
}

func (list *List) Delete(val *RedisObj) {
	list.DelNode(list.Find(val))
}
