package metrics

// TODO add the skiplist part of this...
type llNode struct {
	val  expDecaySample
	next *llNode
}

type expLL struct {
	head *llNode
	tail *llNode
	size int
}

var _ sampleStore = &expLL{}

func (e *expLL) Clear() {
	e.head = nil
	e.tail = nil
	e.size = 0
}

func (e *expLL) Max() int64 {
	return e.tail.val.v
}

func (e *expLL) Min() int64 {
	return e.head.val.v
}

func (e *expLL) Size() int {
	return e.size
}

func (e *expLL) Push(s expDecaySample) int {
	tmp := &llNode{val: s}
	e.size++
	if e.head == nil {
		e.head, e.tail = tmp, tmp
		return -1 // no rank yet?
	}
	var i, prev *llNode
	j := 0
	for i = e.head; i.next != nil && i.val.v < s.v; i = i.next {
		prev = i
		j++
	}
	if s.v >= i.val.v {
		next := i.next
		i.next = tmp
		tmp.next = next
		if tmp.next == nil {
			e.tail = tmp
		}
	} else {
		tmp.next = i
		if prev == nil {
			e.head = tmp
		} else {
			prev.next = tmp
		}
	}
	return j
}

func (e *expLL) Pop() expDecaySample {
	if e.head != nil {
		tmp := e.head
		e.head = tmp.next
		tmp.next = nil
		e.size--
		return tmp.val
	}
	return expDecaySample{}
}

func (e *expLL) Values() []expDecaySample {
	ret := make([]expDecaySample, 0, e.size)
	for i := e.head; i != nil; i = i.next {
		ret = append(ret, i.val)
	}
	return ret
}
