package hashmap

import (
	"fmt"
	"sync"
	"sync/atomic"
	"unsafe"
)

const DefaultLoadFactor = 0.75
const MinInitialSize = 16

type HashFunc[K comparable] func(K) uint32

type (
	entry[K comparable, V any] struct {
		key   K
		value V
		hash  uint32
		next  unsafe.Pointer
	}

	bucket[K comparable, V any] struct {
		mu     sync.Mutex
		length int32
		head   unsafe.Pointer
		frozen uintptr
	}

	hMap[K comparable, V any] struct {
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
		mu       sync.RWMutex
		resizing uintptr
		hashMap  unsafe.Pointer
		old      unsafe.Pointer
	}
)

func (b *bucket[K, V]) search(key K) (prev, cur *entry[K, V]) {
	p := atomic.LoadPointer(&b.head)
	for p != nil {
		cur = (*entry[K, V])(p)
		if cur.key == key {
			return prev, cur
		}
		prev = cur
		p = atomic.LoadPointer(&cur.next)
	}
	return nil, nil
}

func (b *bucket[K, V]) put(e *entry[K, V], update bool) (bool, bool) {
	if b.mu.TryLock() {
		if b.frozen != uintptr(0) {
			b.mu.Unlock()
			return false, false
		}
		_, target := b.search(e.key)
		if target != nil {
			if update {
				target.value = e.value
			}
			b.mu.Unlock()
			return true, false
		}
		atomic.CompareAndSwapPointer(&e.next, nil, b.head)
		atomic.CompareAndSwapPointer(&b.head, b.head, unsafe.Pointer(e))
		atomic.AddInt32(&b.length, 1)
		b.mu.Unlock()
		return true, true
	}
	return false, false
}

func (b *bucket[K, V]) delete(key K) (bool, bool) {
	if b.mu.TryLock() {
		if b.frozen != uintptr(0) {
			b.mu.Unlock()
			return false, false
		}
		prev, target := b.search(key)

		if target == nil {
			b.mu.Unlock()
			return true, false
		}
		if prev != nil {
			atomic.CompareAndSwapPointer(&prev.next, unsafe.Pointer(target), target.next)
		} else {
			atomic.CompareAndSwapPointer(&b.head, unsafe.Pointer(target), target.next)
		}
		b.mu.Unlock()
		return true, true
	}
	return false, false
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
			sync.Mutex{},
			0,
			nil,
			uintptr(0),
		})
	}
	hm.loadFactor = loadFactor
	hm.growThreshold = uint32(float32(cap) * loadFactor)
	hm.shrinkThreshold = uint32(float32(cap) * (1 - loadFactor))
	hm.hashFunc = hashFunc
	return hm
}

func (hm *hMap[K, V]) createEntry(key K, value V) *entry[K, V] {

	e := &entry[K, V]{
		key:   key,
		value: value,
		hash:  hm.hashFunc(key),
		next:  nil,
	}
	return e
}

func (hm *hMap[K, V]) put(e *entry[K, V]) bool {
	idx := e.hash & hm.mask
	b := (*bucket[K, V])(atomic.LoadPointer(&hm.buckets[idx]))

	if done, ok := b.put(e, true); done {
		if ok {
			atomic.AddInt32(&hm.length, 1)
		}
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
	_, target := b.search(key)
	if target != nil {
		return target.value, true
	}
	return value, false
}

func (hm *hMap[K, V]) remove(key K, b *bucket[K, V]) (bool, bool) {
	done, ok := b.delete(key)
	if done && ok {
		atomic.AddInt32(&hm.length, -1)
	}
	return done, ok
}

func (hm *hMap[K, V]) keys() []K {
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

func (m *Map[K, V]) getHashMap() *hMap[K, V] {
	return (*hMap[K, V])(atomic.LoadPointer(&m.hashMap))
}

func (m *Map[K, V]) getOldMap() *hMap[K, V] {
	return (*hMap[K, V])(atomic.LoadPointer(&m.old))
}

func (m *Map[K, V]) readyGrow() bool {
	hashMap := m.getHashMap()
	if uint32(hashMap.length) > hashMap.growThreshold && atomic.CompareAndSwapUintptr(&m.resizing, uintptr(0), uintptr(1)) {
		m.mu.Lock()
		defer m.mu.Unlock()
		newCap := m.Cap() << 1
		newHashMap := NewHashMap[K, V](newCap, hashMap.hashFunc, hashMap.loadFactor)
		if ok := atomic.CompareAndSwapPointer(&m.old, nil, unsafe.Pointer(hashMap)); !ok {
			return false
		}
		if ok := atomic.CompareAndSwapPointer(&m.hashMap, unsafe.Pointer(hashMap), unsafe.Pointer(newHashMap)); !ok {
			return false
		}
		go m.resize(hashMap)
		return true
	}
	return false
}

func (m *Map[K, V]) readyShrink() bool {
	hashMap := m.getHashMap()
	shouldShrink := uint32(hashMap.length) < hashMap.shrinkThreshold && hashMap.cap > MinInitialSize
	if shouldShrink && atomic.CompareAndSwapUintptr(&m.resizing, uintptr(0), uintptr(1)) {
		newCap := m.Cap() >> 1
		newHashMap := NewHashMap[K, V](newCap, hashMap.hashFunc, hashMap.loadFactor)
		if ok := atomic.CompareAndSwapPointer(&m.old, nil, unsafe.Pointer(hashMap)); !ok {
			return false
		}
		if ok := atomic.CompareAndSwapPointer(&m.hashMap, unsafe.Pointer(hashMap), unsafe.Pointer(newHashMap)); !ok {
			return false
		}
		go m.resize(hashMap)
		return true
	}
	return false
}

func (m *Map[K, V]) Put(key K, value V) bool {
	m.mu.Lock()
	for {
		hm := m.getHashMap()
		e := hm.createEntry(key, value)
		done := hm.put(e)
		if !done {
			continue
		}
		m.mu.Unlock()
		m.readyGrow()
		return true
	}

}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	m.mu.RLock()
	defer m.mu.Unlock()
	hashMap := m.getHashMap()
	value, ok = hashMap.get(key)
	return
}

func (m *Map[K, V]) getBucket(key K) (int, *bucket[K, V], unsafe.Pointer) {
	var flag int
	if m.resizing == uintptr(1) {
		flag = 3
		old := m.getOldMap()
		if old != nil {
			hash := old.hashFunc(key)
			idx := hash & old.mask
			b := (*bucket[K, V])(atomic.LoadPointer(&old.buckets[idx]))
			if b.frozen == uintptr(0) {
				m.rehash(b)
				flag = 1
			} else {
				b.mu.Lock()
				b.head = nil
				b.mu.Unlock()
			}
		}
	}

	hm := m.getHashMap()
	hash := hm.hashFunc(key)
	idx := hash & hm.mask
	return flag, (*bucket[K, V])(atomic.LoadPointer(&hm.buckets[idx])), m.old
}

func (m *Map[K, V]) Remove(key K) bool {
	m.mu.Lock()
	for {
		hashMap := m.getHashMap()
		i, b, p := m.getBucket(key)
		done, ok := hashMap.remove(key, b)
		if done {
			if ok {
				m.mu.Unlock()
				m.readyShrink()
				return ok
			} else {
				fmt.Println(i, " ", b, " ", p)
			}
			m.mu.Unlock()
			return ok
		}
	}
}

func (m *Map[K, V]) Keys() []K {
	hashMap := m.getHashMap()
	return hashMap.keys()
}

func (m *Map[K, V]) Values() []V {
	hashMap := m.getHashMap()
	return hashMap.values()
}

func (m *Map[K, V]) Len() int32 {
	hashMap := m.getHashMap()
	return hashMap.len()
}

func (m *Map[K, V]) Cap() uint32 {
	hashMap := m.getHashMap()
	return hashMap.cap
}

func (m *Map[K, V]) resize(oldHashMap *hMap[K, V]) {
	oldBuckets := oldHashMap.buckets
	for i := range oldBuckets {
		b := (*bucket[K, V])(atomic.LoadPointer(&oldBuckets[i]))
		m.rehash(b)
	}
	atomic.StoreUintptr(&m.resizing, uintptr(0))
	atomic.StorePointer(&m.old, nil)
}

func (m *Map[K, V]) rehash(b *bucket[K, V]) bool {
	if !atomic.CompareAndSwapUintptr(&b.frozen, uintptr(0), uintptr(1)) {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.head
	entries := make([]*entry[K, V], 0, b.length)
	for p != nil {
		cur := (*entry[K, V])(p)
		entries = append(entries, cur)
		p = cur.next
	}
	hm := m.getHashMap()
	for i := 0; i < len(entries); {
		atomic.StorePointer(&entries[i].next, nil)
		if done := hm.put(entries[i]); done {
			i++
		}
	}
	atomic.StoreUintptr(&b.frozen, uintptr(2))
	return true
}

func NewMap[K comparable, V any](cap uint32, hashFunc HashFunc[K], loadFactor float32) *Map[K, V] {
	m := new(Map[K, V])
	m.resizing = uintptr(0)
	m.hashMap = unsafe.Pointer(NewHashMap[K, V](cap, hashFunc, loadFactor))
	m.old = nil
	return m
}
