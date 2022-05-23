package hashmap

import (
	"sync"
	"sync/atomic"
	"unsafe"
)

const DefaultLoadFactor = 0.75

type HashFunc[K comparable] func(K) uint32

type (
	entry[K comparable, V any] struct {
		key   K
		value V
		hash  uint32
		next  unsafe.Pointer
		prev  unsafe.Pointer
	}

	bucket[K comparable, V any] struct {
		length   int32
		head     unsafe.Pointer
		deleting uintptr
		frozen   uintptr
	}

	hMap[K comparable, V any] struct {
		mu     sync.RWMutex
		mask   uint32
		cap    uint32
		length int32

		buckets []unsafe.Pointer

		loadFactor      float32
		growThreshold   uint32
		shrinkThreshold uint32
		hashFunc        HashFunc[K]
	}

	Map[K comparable, V any] struct {
		resizing uintptr
		hashMap  *hMap[K, V]
		old      unsafe.Pointer
	}
)

func (b *bucket[K, V]) search(key K) (*entry[K, V], *entry[K, V], *entry[K, V]) {
	for {
		cnt := int32(0)
		p := atomic.LoadPointer(&b.head)
		// p节点被其他节点删除，搜索中断，导致没有找到目标节点
		for p != nil {
			cur := (*entry[K, V])(p)
			if cur.key == key {
				return (*entry[K, V])(cur.prev), cur, (*entry[K, V])(cur.next)
			}
			cnt++
			p = atomic.LoadPointer(&cur.next)
		}
		if cnt < b.length {
			continue
		} else {
			return nil, nil, (*entry[K, V])(b.head)
		}
	}
}

func (b *bucket[K, V]) put(e *entry[K, V]) bool {
	if b.frozen != uintptr(0) {
		return false
	}
	for {
		prev, target, next := b.search(e.key)

		if target != nil {
			target.value = e.value
			return true
		}
		e.prev = unsafe.Pointer(prev)
		e.next = unsafe.Pointer(next)

		if !atomic.CompareAndSwapPointer(&b.head, unsafe.Pointer(next), unsafe.Pointer(e)) {
			continue
		}

		if next != nil {
			if !atomic.CompareAndSwapPointer(&next.prev, unsafe.Pointer(prev), unsafe.Pointer(e)) {
				continue
			}
		}
		atomic.AddInt32(&b.length, 1)
		return true
	}
}

func (b *bucket[K, V]) delete(target *entry[K, V]) bool {
	if b.frozen != uintptr(0) {
		return false
	}
	for {
		if !atomic.CompareAndSwapUintptr(&b.deleting, uintptr(0), uintptr(1)) {
			continue
		}
		prev := (*entry[K, V])(target.prev)
		next := (*entry[K, V])(target.next)
		if prev != nil {
			if !atomic.CompareAndSwapPointer(&prev.next, unsafe.Pointer(target), unsafe.Pointer(next)) {
				continue
			}
		} else {
			if !atomic.CompareAndSwapPointer(&b.head, unsafe.Pointer(target), unsafe.Pointer(next)) {
				continue
			}
		}
		if next != nil {
			if !atomic.CompareAndSwapPointer(&next.prev, unsafe.Pointer(target), unsafe.Pointer(prev)) {
				continue
			}
		}
		atomic.StorePointer(&target.next, nil)
		atomic.AddInt32(&b.length, -1)
		return true
	}
}

func NewHashMap[K comparable, V any](cap uint32, hashFunc HashFunc[K], loadFactor float32) *hMap[K, V] {
	if loadFactor == 0 {
		loadFactor = DefaultLoadFactor
	}
	hm := new(hMap[K, V])
	hm.mask = cap - uint32(1)
	hm.length = 0
	hm.cap = cap
	hm.buckets = make([]unsafe.Pointer, cap, cap)
	for i := range hm.buckets {
		hm.buckets[i] = unsafe.Pointer(&bucket[K, V]{
			0,
			nil,
			uintptr(0),
			uintptr(0),
		})
	}
	hm.loadFactor = loadFactor
	hm.growThreshold = uint32(float32(cap) * loadFactor)
	hm.shrinkThreshold = uint32(float32(cap) * (1 - loadFactor))
	hm.hashFunc = hashFunc
	//return &hMap[K, V]{
	//	mask:            cap - uint32(1),
	//	length:          0,
	//	cap:             cap,
	//	buckets:         make([]unsafe.Pointer, cap),
	//	loadFactor:      loadFactor,
	//	growThreshold:   uint32(float32(cap) * loadFactor),
	//	shrinkThreshold: uint32(float32(cap) * (1 - loadFactor)),
	//  rehashIndex: 	 -1,
	//	hashFunc:        hashFunc,
	//}
	return hm
}

func (hm *hMap[K, V]) createEntry(key K, value V, hash uint32) *entry[K, V] {

	e := &entry[K, V]{
		key:   key,
		value: value,
		hash:  hash,
		next:  nil,
		prev:  nil,
	}
	return e
}

func (hm *hMap[K, V]) put(key K, value V) bool {
	hash := hm.hashFunc(key)
	idx := hash & hm.mask

	b := (*bucket[K, V])(hm.buckets[idx])
	e := hm.createEntry(key, value, hash)

	if ok := b.put(e); ok {
		atomic.AddInt32(&hm.length, 1)
		return true
	}
	return false
}

func (hm *hMap[K, V]) get(key K) (value V, ok bool) {
	if hm.length == 0 {
		return value, false
	}

	hash := hm.hashFunc(key)
	idx := hash & hm.mask

	b := (*bucket[K, V])(hm.buckets[idx])
	_, target, _ := b.search(key)
	if target != nil {
		return target.value, true
	}
	return value, false
}

func (hm *hMap[K, V]) remove(key K) (value V, ok bool) {

	if hm.length == 0 {
		return value, false
	}

	hash := hm.hashFunc(key)
	idx := hash & hm.mask
	b := (*bucket[K, V])(hm.buckets[idx])

	_, target, _ := b.search(key)

	if target == nil {
		return value, false
	}

	b.delete(target)

	atomic.AddInt32(&hm.length, -1)

	atomic.StoreUintptr(&b.deleting, uintptr(0))
	return value, true
}

func (hm *hMap[K, V]) keys() []K {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	keys := make([]K, 0, hm.length)
	for i := range hm.buckets {
		b := (*bucket[K, V])(hm.buckets[i])
		p := b.head
		for p != nil {
			cur := (*entry[K, V])(p)
			keys = append(keys, cur.key)
			p = cur.next
		}
	}
	return keys
}

func (hm *hMap[K, V]) values() []V {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	values := make([]V, 0, hm.length)
	for i := range hm.buckets {
		b := (*bucket[K, V])(hm.buckets[i])
		p := b.head
		for p != nil {
			cur := (*entry[K, V])(p)
			values = append(values, cur.value)
			p = cur.next
		}
	}
	return values
}

func (hm *hMap[K, V]) len() int32 {
	return hm.length
}

func (hm *hMap[K, V]) readyGrow() bool {
	if uint32(hm.length) > hm.growThreshold {
		return true
	}
	return false
}

func (hm *hMap[K, V]) readyShrink() bool {
	if uint32(hm.length) < hm.growThreshold {
		return true
	}
	return false
}

func (m *Map[K, V]) Put(key K, value V) bool {
	m.hashMap.put(key, value)
	m.readResize()
	return true
}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	value, ok = m.hashMap.get(key)
	return
}

func (m *Map[K, V]) Remove(key K) (value V, ok bool) {
	value, ok = m.hashMap.remove(key)
	return
}

func (m *Map[K, V]) Keys() []K {
	return m.hashMap.keys()
}

func (m *Map[K, V]) Values() []V {
	return m.hashMap.values()
}

func (m *Map[K, V]) Len() int32 {
	return m.hashMap.len()
}

func (m *Map[K, V]) Cap() uint32 {
	return m.hashMap.cap
}

func (m *Map[K, V]) readResize() {
	if m.hashMap.readyGrow() {
		m.resize(m.hashMap.cap << 1)
	} else if m.hashMap.readyShrink() {
		m.resize(m.hashMap.cap >> 1)
	}
}

func (m *Map[K, V]) resize(newCap uint32) {
	// 策略：
	// 1. 创建调整大小后新的buckets
	// 2. 遍历并冻结旧的桶
	// 3. 获取各个桶中的节点
	// 4. rehash桶中的节点
	// 5. 根据新的索引位置，插入新的桶中
	atomic.CompareAndSwapUintptr(&m.resizing, uintptr(0), uintptr(1))
	defer atomic.StoreUintptr(&m.resizing, uintptr(0))

	newHashMap := NewHashMap[K, V](newCap, m.hashMap.hashFunc, m.hashMap.loadFactor)
	m.old = unsafe.Pointer(m.hashMap)

	for i := range m.hashMap.buckets {
		b := (*bucket[K, V])(m.hashMap.buckets[i])
		m.rehash(b, newHashMap, m.hashMap.cap)
		m.hashMap.buckets[i] = nil
	}
	m.hashMap = newHashMap
	atomic.StorePointer(&m.old, nil)
}

func (m *Map[K, V]) rehash(b *bucket[K, V], newHashMap *hMap[K, V], oldCap uint32) {
	atomic.CompareAndSwapUintptr(&b.frozen, uintptr(0), uintptr(1))

	p := b.head

	for p != nil {
		cur := (*entry[K, V])(p)
		p = cur.next
		idx := cur.hash & newHashMap.mask
		b1 := (*bucket[K, V])(newHashMap.buckets[idx])
		b1.put(cur)
		newHashMap.length++
	}
}

func NewMap[K comparable, V any](cap uint32, hashFunc HashFunc[K], loadFactor float32) *Map[K, V] {
	m := new(Map[K, V])
	m.resizing = uintptr(0)
	m.hashMap = NewHashMap[K, V](cap, hashFunc, loadFactor)
	m.old = nil
	return m
}
