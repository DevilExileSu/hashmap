package hashmap

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"unsafe"
)

func (e *entry[K, V]) String() string {
	return fmt.Sprintf("key: %v, value: %v, next: %v\n", e.key, e.value, e.next)
}

func TestEntry(t *testing.T) {
	e := entry[int, int]{
		key:   1,
		value: 1,
		next:  nil,
	}
	e1 := entry[int, int]{
		key:   2,
		value: 2,
		next:  nil,
	}
	e2 := entry[int, string]{
		key:   3,
		value: "test",
		next:  nil,
	}

	atomic.CompareAndSwapPointer(&e.next, nil, unsafe.Pointer(&e1))
	atomic.CompareAndSwapPointer(&e1.next, nil, unsafe.Pointer(&e2))
	if e.next != unsafe.Pointer(&e1) {
		t.Fatalf("e.next err, should %v, but %v", unsafe.Pointer(&e1), e.next)
	}
	if e1.next != unsafe.Pointer(&e2) {
		t.Fatalf("e1.next err, should %v, but %v", unsafe.Pointer(&e2), e1.next)
	}

	t.Logf("entry: %v", e)
	t.Logf("entry: %v", e1)
	t.Logf("entry: %v", e2)

}

func TestHMapPut(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)

	hash := hm.hashFunc(16)
	idx := hash & hm.mask

	b := (*bucket[int, int])(hm.buckets[idx])
	p := b.head
	i := 0
	for p != nil {
		cur := (*entry[int, int])(p)
		t.Logf("buckets[%v]: %v -> [entry=%v]", idx, i, cur)
		i++
		p = cur.next
	}

	hm.put(16, 16)
	hm.put(32, 32)
	hm.put(0, 1)
	b = (*bucket[int, int])(hm.buckets[idx])
	p = b.head
	i = 0
	for p != nil {
		cur := (*entry[int, int])(p)
		t.Logf("buckets[%v]: %v -> [entry=%v]", idx, i, cur)
		i++
		p = cur.next
	}
}

func TestHMapGet(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)

	if v, ok := hm.get(16); v != 10 || !ok {
		t.Fatalf("err get, should: %v, but: %v", 10, v)
	}

	if v, ok := hm.get(0); v != 0 || !ok {
		t.Fatalf("err get, should: %v, but: %v", 0, v)
	}

	if v, ok := hm.get(5); ok {
		t.Fatalf("err get, should: nil, but: %v", v)
	}
}

func TestHMapKeysAndValues(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)
	t.Logf("hm.length = %v", hm.length)
	keys := hm.keys()
	t.Logf("keys = %v", keys)
	values := hm.values()
	t.Logf("values = %v", values)
}

func TestHMapRemove(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)
	t.Logf("hm.length = %v", hm.length)
	keys := hm.keys()
	t.Logf("keys = %v", keys)
	values := hm.values()
	t.Logf("values = %v", values)
	hm.remove(32)
	t.Logf("hm.length = %v", hm.length)
	keys = hm.keys()
	t.Logf("keys = %v", keys)
	values = hm.values()
	t.Logf("values = %v", values)
	hm.remove(0)
	t.Logf("hm.length = %v", hm.length)
	keys = hm.keys()
	t.Logf("keys = %v", keys)
	values = hm.values()
	t.Logf("values = %v", values)

}

func TestHMapConcurrencyPutAndRemove(t *testing.T) {

	hm := NewHashMap[string, int](10000, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				hm.put(key, i)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	keys := hm.keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v", hm.len(), len(keys))
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				hash := hm.hashFunc(key)
				idx := hash & hm.mask
				_, ok := hm.remove(key)
				if !ok {
					t.Logf("idx=%v, key=%v", idx, key)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	t.Log()
	keys = hm.keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v", hm.length, len(keys))
}

func TestMapRsize(t *testing.T) {
	hm := NewMap[string, int](16, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	for i := 0; i < 100; i++ {
		for j := 0; j < 10000; j++ {
			key := fmt.Sprintf("%v-[%v]", i+1, j)
			hm.Put(key, j)
			for hm.resizing != uintptr(0) {
				continue
			}
		}
		t.Logf("hm.length = %v, hm.cap = %v", hm.Len(), hm.Cap())
	}
}
