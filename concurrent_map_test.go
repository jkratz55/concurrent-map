package concurrentmap

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
)

func TestNew(t *testing.T) {
	m := New[string](16)
	if m.Size() != 0 {
		t.Errorf("expected map to have size of 0, instead got %d", m.Size())
	}
	if m.shardCount != 16 {
		t.Errorf("expected map to have 16 shards, instead got %d", m.shardCount)
	}
}

func TestNew_ZeroShards(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("A panic was expected but did not occur")
		}
	}()
	_ = New[string](0)
}

func TestNew_ExceedMaxShards(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("A panic was expected but did not occur")
		}
	}()
	_ = New[string](2147483648)
}

func TestConcurrentMap_Put(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "Some Random Product Name")

	shard := m.getShard("11111111")
	val, ok := shard.items["11111111"]
	if !ok {
		t.Errorf("entry not found in map")
	}
	if val != "Some Random Product Name" {
		t.Errorf("incorrect value")
	}
	if m.Size() != 1 {
		t.Errorf("map reported the wrong size")
	}
}

func TestConcurrentMap_Get_ValueExists(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "Some Random Product Name")

	val, ok := m.Get("11111111", true)
	if !ok {
		t.Errorf("entry not found in map")
	}
	if val != "Some Random Product Name" {
		t.Errorf("incorrect value")
	}
	if m.Size() != 1 {
		t.Errorf("map reported the wrong size")
	}
}

func TestConcurrentMap_Get_NoValue(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "Some Random Product Name")

	_, ok := m.Get("2222222", true)
	if ok {
		t.Errorf("entry found but wasn't expected")
	}
}

func TestConcurrentMap_GetUnsafe(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "Some Random Product Name")

	val, ok := m.Get("11111111", false)
	if !ok {
		t.Errorf("entry not found in map")
	}
	if val != "Some Random Product Name" {
		t.Errorf("incorrect value")
	}
	if m.Size() != 1 {
		t.Errorf("map reported the wrong size")
	}
}

func TestConcurrentMap_PutIfAbsent(t *testing.T) {
	m := New[string](32)
	ok := m.PutIfAbsent("11111111", "$99.99")

	if !ok {
		t.Errorf("expected value to be put in map")
	}
	if m.Size() != 1 {
		t.Errorf("expected size of 1 but got %d", m.Size())
	}

	ok = m.PutIfAbsent("11111111", "79.99")
	if ok {
		t.Errorf("value that was already present unexpectently overriden")
	}
	val, ok := m.Get("11111111", true)
	if !ok {
		t.Errorf("failed to retrieve record from map")
	}
	if val != "$99.99" {
		t.Errorf("value that was already present unexpectently overriden, expected $99.99, got %s", val)
	}
}

func TestConcurrentMap_Remove(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "$99.99")
	m.Put("22222222", "$89.99")

	m.Remove("11111111")
	if _, ok := m.Get("11111111", true); ok {
		t.Errorf("11111111 was removed from the Map but still present")
	}
	if m.Size() != 1 {
		t.Errorf("expected size of 1 but got %d", m.Size())
	}
}

func TestConcurrentMap_Pop(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "$99.99")
	m.Put("22222222", "$89.99")

	val, ok := m.Pop("11111111")
	if !ok {
		t.Errorf("entry with key 11111111 was expected in map but not found")
	}
	if val != "$99.99" {
		t.Errorf("expected value of $99.99 but got %s", val)
	}

	if _, ok := m.Get("11111111", true); ok {
		t.Errorf("11111111 was removed from the Map but still present")
	}
	if m.Size() != 1 {
		t.Errorf("expected size of 1 but got %d", m.Size())
	}
}

func TestConcurrentMap_Upsert_NoValueExist(t *testing.T) {
	m := New[string](32)

	f := UpsertFunc[string](func(exist bool, valueInMap string, newValue string) string {
		if exist {
			t.Errorf("upsert func recieved true for exist, but record should not exist")
		}
		if valueInMap != "" {
			t.Errorf("expected empty string for valueInMap")
		}
		return newValue
	})

	m.Upsert("11111111", "$9.99", f)

	val, ok := m.Get("11111111", true)
	if !ok {
		t.Errorf("Entry with key 11111111 should exist but wasn't found")
	}
	if val != "$9.99" {
		t.Errorf("expected value for key 11111111 of $9.99 but got %s", val)
	}
}

func TestConcurrentMap_Upsert_ValueExist(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "$9.99")

	f := UpsertFunc[string](func(exist bool, valueInMap string, newValue string) string {
		if !exist {
			t.Errorf("upsert func recieved false for exist, but record should exist")
		}
		if valueInMap != "$9.99" {
			t.Errorf("expected empty string for valueInMap")
		}
		return newValue
	})

	m.Upsert("11111111", "$19.99", f)

	val, ok := m.Get("11111111", true)
	if !ok {
		t.Errorf("Entry with key 11111111 should exist but wasn't found")
	}
	if val != "$19.99" {
		t.Errorf("expected $19.99 but got %s", val)
	}
}

func TestConcurrentMap_Clear(t *testing.T) {
	m := New[string](32)
	for i := 0; i < 100000; i++ {
		m.Put(strconv.Itoa(i), strconv.Itoa(i))
	}

	m.Clear()
	if m.Size() != 0 {
		t.Errorf("expect 0 size of map, got %d", m.Size())
	}
}

func TestConcurrentMap_ShardStats(t *testing.T) {

	m := New[string](32)
	for i := 0; i < 100000; i++ {
		m.Put(strconv.Itoa(i), strconv.Itoa(i))
	}

	count := 0
	stats := m.ShardStats()
	for _, c := range stats {
		count += c
	}

	if count != 100000 {
		t.Errorf("expected total of 100000 elements in map, but got %d", count)
	}
}

func TestConcurrentMap_ContainsKey(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "Test Value")
	m.Put("22222222", "Test Value")

	if !m.ContainsKey("11111111") {
		t.Errorf("key 11111111 was added to map but ContainsKey returned false")
	}
	if !m.ContainsKey("22222222") {
		t.Errorf("key 22222222 was added to map but ContainsKey returned false")
	}
}

func TestConcurrentMap_Keys(t *testing.T) {
	m := New[string](32)
	m.Put("11111111", "Test Value")
	m.Put("22222222", "Test Value")
	m.Put("33333333", "Test Value")
	m.Put("44444444", "Test Value")
	m.Put("55555555", "Test Value")

	expected := []string{"11111111", "22222222", "33333333", "44444444", "55555555"}
	keys := m.Keys()

	if !cmp.Equal(expected, keys, cmpopts.SortSlices(func(x, y string) bool {
		return x < y
	})) {
		t.Errorf("keys didn't returned expected keys")
	}
}

func TestConcurrentMap_Range(t *testing.T) {

	called := 0
	f := RangeFunc[string](func(key string, val string) {
		called++
	})

	m := New[string](32)
	for i := 0; i < 100000; i++ {
		m.Put(strconv.Itoa(i), strconv.Itoa(i))
	}

	m.Range(f)

	if called != 100000 {
		t.Errorf("expected RangeFunc to be called 100000 times, but was called %d", called)
	}
}

func TestConcurrentMap_Iter(t *testing.T) {

	count := 0
	ch := make(chan Entry[string], 1000)

	m := New[string](32)
	for i := 0; i < 100000; i++ {
		m.Put(strconv.Itoa(i), strconv.Itoa(i))
	}

	go func() {
		m.Iter(ch)
	}()

	for range ch {
		count++
	}

	if count != 100000 {
		t.Errorf("expected to have processed 100000 elements, but got %d", count)
	}
}
