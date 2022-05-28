package hashmap

import (
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

// Attempt to lock, if failed, return (false, false).
// If the bucket is frozen, it means that resize has occurred, return (false, false)
// to notify Map to obtain a new bucket for put operation.
// (true, false): The operation succeeded but no new nodes were created
// (true, true): The operation was successful and a node was created
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

// Similar to put method
// Attempt to lock, if failed, return (false, false).
// If the bucket is frozen, it means that resize has occurred, return (false, false)
// to notify Map to obtain a new bucket for put operation.
// (true, false): The operation succeeded but no nodes were deleted
// (true, true): The operation succeeds and the node is deleted
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

// Get the bucket where the entry is located according to the hash
// If the operation is successful and a node is created，hm.length++
// If the operation fails, the lock is not obtained,
// or the hMap is not new at this time, return false.
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

func (hm *hMap[K, V]) putWithoutUpdating(e *entry[K, V]) bool {
	idx := e.hash & hm.mask
	b := (*bucket[K, V])(atomic.LoadPointer(&hm.buckets[idx]))

	if done, ok := b.put(e, false); done {
		if ok {
			atomic.AddInt32(&hm.length, 1)
		}
		return true
	}
	return false
}

func (hm *hMap[K, V]) getWithBucket(key K, b *bucket[K, V]) (value V, ok bool) {
	if b.length == 0 {
		return value, false
	}
	_, target := b.search(key)
	if target != nil {
		return target.value, true
	}
	return value, false
}

func (hm *hMap[K, V]) get(key K) (value V, ok bool) {
	if hm.length == 0 {
		return value, false
	}

	hash := hm.hashFunc(key)
	idx := hash & hm.mask

	b := (*bucket[K, V])(atomic.LoadPointer(&hm.buckets[idx]))
	_, target := b.search(key)
	if target != nil {
		return target.value, true
	}
	return value, false
}

func (hm *hMap[K, V]) remove(key K, b *bucket[K, V]) (bool, bool) {
	done, ok := b.delete(key)
	if done {
		if ok {
			atomic.AddInt32(&hm.length, -1)
		}
	}
	return done, ok
}

func (hm *hMap[K, V]) getBucket(key K) *bucket[K, V] {
	hash := hm.hashFunc(key)
	idx := hash & hm.mask
	return (*bucket[K, V])(atomic.LoadPointer(&hm.buckets[idx]))

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

func (m *Map[K, V]) readyGrow(hm *hMap[K, V]) bool {
	if uint32(hm.length) > hm.growThreshold && atomic.CompareAndSwapUintptr(&m.resizing, uintptr(0), uintptr(1)) {
		m.mu.Lock()
		defer m.mu.Unlock()
		newCap := hm.cap << 1
		newHashMap := NewHashMap[K, V](newCap, hm.hashFunc, hm.loadFactor)
		if ok := atomic.CompareAndSwapPointer(&m.old, m.old, unsafe.Pointer(hm)); !ok {
			return false
		}
		if ok := atomic.CompareAndSwapPointer(&m.hashMap, unsafe.Pointer(hm), unsafe.Pointer(newHashMap)); !ok {
			return false
		}
		go m.resize(hm)
		return true
	}
	return false
}

func (m *Map[K, V]) readyShrink(hm *hMap[K, V]) bool {

	shouldShrink := uint32(hm.length) < hm.shrinkThreshold && hm.cap > MinInitialSize
	if shouldShrink && atomic.CompareAndSwapUintptr(&m.resizing, uintptr(0), uintptr(1)) {
		m.mu.Lock()
		defer m.mu.Unlock()
		newCap := hm.cap >> 1
		newHashMap := NewHashMap[K, V](newCap, hm.hashFunc, hm.loadFactor)
		if ok := atomic.CompareAndSwapPointer(&m.old, m.old, unsafe.Pointer(hm)); !ok {
			return false
		}
		if ok := atomic.CompareAndSwapPointer(&m.hashMap, unsafe.Pointer(hm), unsafe.Pointer(newHashMap)); !ok {
			return false
		}
		go m.resize(hm)
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
		m.readyGrow(hm)
		return true
	}

}

func (m *Map[K, V]) Get(key K) (value V, ok bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, hm := m.getBucket(key)
	value, ok = hm.getWithBucket(key, b)
	return
}

// Remove
// Find the bucket where the key is located in the old hMap.
// If the bucket has not been rehashed, call rehash()
// remove() in new new hMap
func (m *Map[K, V]) Remove(key K) bool {
	m.mu.Lock()
	for {
		b, hm := m.getBucket(key)
		done, ok := hm.remove(key, b)
		if done {
			if ok {
				m.mu.Unlock()
				m.readyShrink(hm)
				return ok
			}
			m.mu.Unlock()
			return ok
		}
	}
}

// Promise to get the rehashed bucket with the latest hashmap
func (m *Map[K, V]) getBucket(key K) (*bucket[K, V], *hMap[K, V]) {
	if m.resizing == uintptr(1) {
		old := m.getOldMap()
		if old != nil {
			b := old.getBucket(key)
			// The old bucket has not been rehashed, call rehash()
			if b.frozen == uintptr(0) {
				m.rehash(b)
			}
			// Block waiting for rehash to end
			if b.frozen != uintptr(2) {
				b.mu.Lock()
				b.head = nil
				b.mu.Unlock()
			}
		}
	}
	hm := m.getHashMap()
	return hm.getBucket(key), hm
}

func (m *Map[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for {
		resizing := atomic.LoadUintptr(&m.resizing)
		if resizing == uintptr(0) {
			break
		}
	}
	hashMap := m.getHashMap()
	return hashMap.keys()
}

func (m *Map[K, V]) Values() []V {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for {
		resizing := atomic.LoadUintptr(&m.resizing)
		if resizing == uintptr(0) {
			break
		}
	}
	hashMap := m.getHashMap()
	return hashMap.values()
}

func (m *Map[K, V]) Len() int32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for {
		resizing := atomic.LoadUintptr(&m.resizing)
		if resizing == uintptr(0) {
			break
		}
	}
	hashMap := m.getHashMap()
	return hashMap.len()
}

func (m *Map[K, V]) Cap() uint32 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for {
		resizing := atomic.LoadUintptr(&m.resizing)
		if resizing == uintptr(0) {
			break
		}
	}
	hashMap := m.getHashMap()
	return hashMap.cap
}

// Rehashing the nodes of each bucket of the old hMap
func (m *Map[K, V]) resize(oldHashMap *hMap[K, V]) {
	oldBuckets := oldHashMap.buckets
	for i := range oldBuckets {
		b := (*bucket[K, V])(atomic.LoadPointer(&oldBuckets[i]))
		m.rehash(b)
	}
	// After all nodes are rehashed, throw away the old hMap
	atomic.StorePointer(&m.old, nil)
	atomic.StoreUintptr(&m.resizing, uintptr(0))
}

func (m *Map[K, V]) rehash(b *bucket[K, V]) bool {
	if b.mu.TryLock() {
		// frozen = 1: bucket can not write or delete
		if !atomic.CompareAndSwapUintptr(&b.frozen, uintptr(0), uintptr(1)) {
			b.mu.Unlock()
			return false
		}
		p := b.head
		entries := make([]*entry[K, V], 0, b.length)
		hm := m.getHashMap()
		for p != nil {
			cur := (*entry[K, V])(p)
			entries = append(entries, cur)
			p = cur.next
		}

		// new bucket may be executing write or delete.
		for i := 0; i < len(entries); {
			atomic.StorePointer(&entries[i].next, nil)
			if done := hm.putWithoutUpdating(entries[i]); done {
				i++
			}
		}
		// frozen = 2: bucket rehash complete
		atomic.StoreUintptr(&b.frozen, uintptr(2))
		b.mu.Unlock()
		return true
	}
	return false
}

func NewMap[K comparable, V any](cap uint32, hashFunc HashFunc[K], loadFactor float32) *Map[K, V] {
	m := new(Map[K, V])
	m.resizing = uintptr(0)
	m.hashMap = unsafe.Pointer(NewHashMap[K, V](cap, hashFunc, loadFactor))
	m.old = nil
	return m
}
