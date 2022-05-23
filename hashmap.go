package hashmap

import (
	"runtime"
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

	hashMap[K comparable, V any] struct {
		mu     sync.RWMutex
		mask   uint32
		cap    uint32
		length int32

		buckets []unsafe.Pointer

		loadFactor      float32
		growThreshold   uint32
		shrinkThreshold uint32
		resizing        uintptr
		old             unsafe.Pointer
		hashFunc        HashFunc[K]
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
	for {
		prev, target, next := b.search(e.key)

		if target != nil {
			target.value = e.value
			return true
		}
		e.prev = unsafe.Pointer(prev)
		e.next = unsafe.Pointer(next)
		if prev == nil {
			if !atomic.CompareAndSwapPointer(&b.head, unsafe.Pointer(next), unsafe.Pointer(e)) {
				continue
			}
		} else {
			if !atomic.CompareAndSwapPointer(&prev.next, unsafe.Pointer(next), unsafe.Pointer(e)) {
				continue
			}
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

func (b *bucket[K, V]) delete(target *entry[K, V]) {
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
		return
	}
}

func NewHashMap[K comparable, V any](cap uint32, hashFunc HashFunc[K], loadFactor float32) *hashMap[K, V] {
	if loadFactor == 0 {
		loadFactor = DefaultLoadFactor
	}
	m := new(hashMap[K, V])
	m.mask = cap - uint32(1)
	m.length = 0
	m.cap = cap
	m.buckets = make([]unsafe.Pointer, cap, cap)
	for i := range m.buckets {
		m.buckets[i] = unsafe.Pointer(&bucket[K, V]{
			0,
			nil,
			uintptr(0),
			uintptr(0),
		})
	}
	m.loadFactor = loadFactor
	m.growThreshold = uint32(float32(cap) * loadFactor)
	m.shrinkThreshold = uint32(float32(cap) * (1 - loadFactor))
	m.resizing = uintptr(0)
	m.hashFunc = hashFunc
	m.old = nil
	//return &hashMap[K, V]{
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
	return m
}

func (hm *hashMap[K, V]) createEntry(key K, value V, hash uint32) *entry[K, V] {

	e := &entry[K, V]{
		key:   key,
		value: value,
		hash:  hash,
		next:  nil,
		prev:  nil,
	}
	return e
}

func (hm *hashMap[K, V]) put(key K, value V) bool {
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

func (hm *hashMap[K, V]) get(key K) (value V, ok bool) {
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

func (hm *hashMap[K, V]) Remove(key K) (value V, ok bool) {

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

func (hm *hashMap[K, V]) Keys() []K {
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

func (hm *hashMap[K, V]) Values() []V {
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

func (hm *hashMap[K, V]) Len() int32 {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.length
}

func (hm *hashMap[K, V]) Cap() uint32 {
	hm.mu.RLock()
	defer hm.mu.RUnlock()
	return hm.cap
}

func (hm *hashMap[K, V]) resize(newCap uint32) {
	// 策略：
	// 1. 创建调整大小后新的buckets
	// 2. 遍历并冻结旧的桶
	// 3. 获取各个桶中的节点
	// 4. rehash桶中的节点
	// 5. 根据新的索引位置，插入新的桶中
	atomic.CompareAndSwapUintptr(&hm.resizing, uintptr(0), uintptr(1))
	defer atomic.StoreUintptr(&hm.resizing, uintptr(0))

	newHashMap := NewHashMap[K, V](newCap, hm.hashFunc, hm.loadFactor)
	newHashMap.old = unsafe.Pointer(hm)
	old := hm
	*hm = *newHashMap

	for i := range old.buckets {
		b := (*bucket[K, V])(old.buckets[i])
		hm.rehash(b, i, old.cap)
	}

	old = nil
	atomic.StorePointer(&hm.old, nil)
	runtime.GC()
}

func (hm *hashMap[K, V]) rehash(b *bucket[K, V], idx int, oldCap uint32) {
	atomic.CompareAndSwapUintptr(&b.frozen, uintptr(0), uintptr(1))
	defer atomic.StoreUintptr(&b.frozen, uintptr(0))

	p := (*entry[K, V])(b.head)
	b1 := (*bucket[K, V])(hm.buckets[idx])
	b2 := (*bucket[K, V])(hm.buckets[idx+int(oldCap)])
	for p != nil {
		if p.hash&oldCap == 0 {
			//hm.put()
			// 加到和原来位置相同的buckets
			b1.put(p)
		} else {
			// 加到 旧位置 + oldCap处的buckets
			b2.put(p)
		}
	}

}

//func (hm *hashMap[K, V]) shrink(b1, b2 *bucket[K, V]) {}
