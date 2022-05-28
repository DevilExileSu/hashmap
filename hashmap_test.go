package hashmap

import (
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"
)

func (e *entry[K, V]) String() string {
	return fmt.Sprintf("key: %v, value: %v, hash: %v next: %v", e.key, e.value, e.hash, e.next)
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
	entries := []*entry[int, int]{
		hm.createEntry(0, 0),
		hm.createEntry(1, 1),
		hm.createEntry(2, 2),
		hm.createEntry(16, 10),
		hm.createEntry(32, 100),
	}

	for i := range entries {
		hm.put(entries[i])
	}

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
	keys := hm.keys()
	t.Logf("%v", keys)
	entries2 := []*entry[int, int]{
		hm.createEntry(0, 1),
		hm.createEntry(16, 16),
		hm.createEntry(32, 32),
	}
	for i := range entries2 {
		hm.put(entries2[i])
	}
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

	entries := []*entry[int, int]{
		hm.createEntry(0, 0),
		hm.createEntry(1, 1),
		hm.createEntry(2, 2),
		hm.createEntry(16, 10),
		hm.createEntry(32, 100),
	}

	for i := range entries {
		hm.put(entries[i])
	}

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
	entries := []*entry[int, int]{
		hm.createEntry(0, 0),
		hm.createEntry(1, 1),
		hm.createEntry(2, 2),
		hm.createEntry(16, 10),
		hm.createEntry(32, 100),
	}

	for i := range entries {
		hm.put(entries[i])
	}

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
	entries := []*entry[int, int]{
		hm.createEntry(0, 0),
		hm.createEntry(1, 1),
		hm.createEntry(2, 2),
		hm.createEntry(16, 10),
		hm.createEntry(32, 100),
	}

	for i := range entries {
		hm.put(entries[i])
	}
	t.Logf("hm.length = %v", hm.length)
	keys := hm.keys()
	t.Logf("keys = %v", keys)
	values := hm.values()
	t.Logf("values = %v", values)
	for _, key := range []int{0, 1, 32} {
		hash := hm.hashFunc(key)
		idx := hash & hm.mask
		b := (*bucket[int, int])(hm.buckets[idx])
		hm.remove(key, b)
	}

	t.Logf("hm.length = %v", hm.length)
	keys = hm.keys()
	t.Logf("keys = %v", keys)
	values = hm.values()
	t.Logf("values = %v", values)

}
func TestHMapPutAndRemove(t *testing.T) {

	hm := NewHashMap[string, int](1000000, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)
	//m := map[string]int{}
	for i := 0; i < 100; i++ {
		for j := 0; j < 10000; j++ {
			key := fmt.Sprintf("%v-[%v]", i+1, j)
			hm.put(hm.createEntry(key, i))
			//m[key] = j
		}
	}
	keys := hm.keys()
	sort.Strings(keys)

	t.Logf("hm.length = %v, length = %v", hm.len(), len(keys))
	for i := 0; i < 100; i++ {
		for j := 0; j < 10000; j++ {
			key := fmt.Sprintf("%v-[%v]", i+1, j)
			hash := hm.hashFunc(key)
			idx := hash & hm.mask
			//delete(m, key)
			b := (*bucket[string, int])(hm.buckets[idx])
			_, ok := hm.remove(key, b)
			if !ok {
				t.Logf("idx=%v, key=%v", idx, key)
			}
		}

	}

	keys = hm.keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v,", hm.len(), len(keys))
}
func TestHMapConcurrencyPutAndRemove(t *testing.T) {

	hm := NewHashMap[string, int](100000, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	wg := sync.WaitGroup{}
	//m := sync.Map{}
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				hm.put(hm.createEntry(key, i))
				//m.Store(key, i)
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
			for i := 0; i < 1000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				hash := hm.hashFunc(key)
				idx := hash & hm.mask
				b := (*bucket[string, int])(hm.buckets[idx])
				_, ok := hm.remove(key, b)
				if !ok {
					t.Logf("idx=%v, key=%v", idx, key)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()

	keys = hm.keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v,", hm.len(), len(keys))
}

func TestMapRsize(t *testing.T) {
	m := NewMap[string, int](16, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	for i := 0; i < 100; i++ {
		for j := 0; j < 10000; j++ {
			key := fmt.Sprintf("%v-%v", i+1, j)
			m.Put(key, j)
		}
	}
	for m.resizing == uintptr(1) {
		time.Sleep(1000)
		continue
	}
	keys := m.Keys()
	sort.Strings(keys)
	t.Logf("hm.length = %v, length = %v, cap = %v,", m.Len(), len(keys), m.Cap())
}

func TestMapPutAndRemove(t *testing.T) {
	m := NewMap[string, int](16, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	for i := 0; i < 100; i++ {
		for j := 0; j < 10000; j++ {
			key := fmt.Sprintf("%v-[%v]", i+1, j)
			m.Put(key, i)
		}
	}

	keys := m.Keys()
	t.Logf("m.length = %v, len(keys) = %v, m.Cap = %v", m.Len(), len(keys), m.Cap())

	for i := 0; i < 100; i++ {
		for j := 0; j < 9999; j++ {
			key := fmt.Sprintf("%v-[%v]", i+1, j)
			ok := m.Remove(key)
			if !ok {
				t.Logf("idx=%v, key=%v", i, key)
			}
		}
	}
	keys = m.Keys()
	t.Logf("m.length = %v, len(keys) = %v, m.Cap = %v", m.Len(), len(keys), m.Cap())

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("%v-[%v]", i+1, 9999)
		if _, ok := m.Get(key); !ok {
			t.Fatal(key)
		}
	}
}

func TestMapConcurrencyPutAndRemove(t *testing.T) {
	m := NewMap[string, int](16, func(key string) uint32 {
		new32 := fnv.New32()
		new32.Write([]byte(key))
		return new32.Sum32()
	}, 0)

	wg := sync.WaitGroup{}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				m.Put(key, i)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	keys := m.Keys()
	sort.Strings(keys)
	t.Logf("m.length = %v, len(keys) = %v, m.Cap = %v", m.Len(), len(keys), m.Cap())
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			for i := 0; i < 10000; i++ {
				key := fmt.Sprintf("%v-[%v]", id+1, i)
				ok := m.Remove(key)
				if !ok {
					t.Logf("idx=%v, key=%v", id, key)
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	keys = m.Keys()

	sort.Strings(keys)
	t.Logf("m.length = %v, len(keys) = %v, m.Cap = %v", m.Len(), len(keys), m.Cap())
}
