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

func TestPut(t *testing.T) {
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

func TestGet(t *testing.T) {
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

func TestKeysAndValues(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)
	t.Logf("hm.length = %v", hm.length)
	keys := hm.Keys()
	t.Logf("keys = %v", keys)
	values := hm.Values()
	t.Logf("values = %v", values)
}

func TestRemove(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)
	t.Logf("hm.length = %v", hm.length)
	keys := hm.Keys()
	t.Logf("keys = %v", keys)
	values := hm.Values()
	t.Logf("values = %v", values)
	hm.Remove(32)
	t.Logf("hm.length = %v", hm.length)
	keys = hm.Keys()
	t.Logf("keys = %v", keys)
	values = hm.Values()
	t.Logf("values = %v", values)
	hm.Remove(0)
	t.Logf("hm.length = %v", hm.length)
	keys = hm.Keys()
	t.Logf("keys = %v", keys)
	values = hm.Values()
	t.Logf("values = %v", values)

}

func TestResize(t *testing.T) {
	hm := NewHashMap[int, int](16, func(key int) uint32 {
		return uint32(key)
	}, 0)
	hm.put(0, 0)
	hm.put(1, 1)
	hm.put(2, 2)
	hm.put(16, 10)
	hm.put(32, 100)

	t.Logf("hm.length = %v, hm.cap = %v", hm.length, hm.Cap())
	keys := hm.Keys()
	t.Logf("keys = %v", keys)
	values := hm.Values()
	t.Logf("values = %v", values)

	newBuckets := make([]unsafe.Pointer, 16*2)
	copy(newBuckets, hm.buckets)
	hm.buckets = newBuckets

	t.Logf("hm.length = %v, hm.cap = %v", hm.length, hm.Cap())
	keys = hm.Keys()
	t.Logf("keys = %v", keys)
	values = hm.Values()
	t.Logf("values = %v", values)
}

func TestHashMap_C(t *testing.T) {

	hm := NewHashMap[string, int](50000, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	wg := sync.WaitGroup{}

	//for i := 0; i < 5; i++ {
	//	for j := 0; j < 1000; j++ {
	//		key := fmt.Sprintf("%v-[%v]", i, j)
	//		hm.put(key, j)
	//	}
	//}

	for i := 0; i < 5; i++ {
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
	keys := hm.Keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v", hm.length, len(keys))
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(id int) {
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				hash := hm.hashFunc(key)
				idx := hash & hm.mask
				_, ok := hm.Remove(key)
				if !ok {
					t.Logf("idx=%v, key=%v", idx, key)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	//for i := 0; i < 5; i++ {
	//	for j := 0; j < 10000; j++ {
	//		key := fmt.Sprintf("%v-[%v]", i, j)
	//		_, ok := hm.Remove(key)
	//		if !ok {
	//			fmt.Println(key)
	//		}
	//	}
	//}

	t.Log()
	keys = hm.Keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v", hm.length, len(keys))
	//for i := range keys {
	//	hash := hm.hashFunc(keys[i])
	//	idx := hash & hm.mask
	//	b := (*bucket[string, int])(hm.buckets[idx])
	//	_, target, _ := hm.search(keys[i], b)
	//	t.Logf("idx=%v, entry=%v, key = %v, prev=%v", idx, target, keys[i], (*entry[string, int])(target.prev))
	//
	//}
}
