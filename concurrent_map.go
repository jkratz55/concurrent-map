package concurrentmap

import (
	"sync"
)

const DefaultShards = 32

type Entry[T any] struct {
	Key   string
	Value T
}

// RangeFunc is a function that gets invoked when ranging over ConcurrentMap
type RangeFunc[T any] func(key string, val T)

// UpsertFunc is a function that is called by Upsert allowing this type to
// determine what value gets inserted/set in the Map.
type UpsertFunc[T any] func(exist bool, valueInMap T, newValue T) T

// ConcurrentMap is a concurrent safe Map that provides the basic features
// of working with Maps: Get, Put, Range, etc.
//
// ConcurrentMap is implemented by sharding the data and using a sync.RWMutex
// within each shard to reduce lock contention. This leads to generally good
// performance, and the number of shards can be customized to optimize performance.
// However, there is a drawback to this implementation. Because operations are atomic
// at the shard level, and not the ConcurrentMap, some methods like Keys, Range,
// and Size may not be accurate enough for logic decisions. This is because shards
// that have already been processed may have been mutated while the method is processing
// other shards.
type ConcurrentMap[T any] struct {
	shards     []*ConcurrentMapShard[T]
	shardCount int
}

// ConcurrentMapShard is a shard holding a portion of the data within
// a ConcurrentMap. Operations within a ConcurrentMapShard are atomic.
type ConcurrentMapShard[T any] struct {
	items map[string]T
	sync.RWMutex
}

// New creates a new ConcurrentMap with the provided number of shards.
//
// The number of shards should be strategically selected based on amount
// of data and profile of the application as write heavy applications are
// more likely to cause lock contention.
func New[T any](shards uint) ConcurrentMap[T] {
	if shards == 0 {
		panic("number of shards must be greater than 0")
	}
	if shards > 2147483647 {
		panic("maximum supported shards is 2147483647")
	}
	m := make([]*ConcurrentMapShard[T], shards)
	for i := 0; i < int(shards); i++ {
		m[i] = &ConcurrentMapShard[T]{
			items: make(map[string]T),
		}
	}
	return ConcurrentMap[T]{
		shards:     m,
		shardCount: int(shards),
	}
}

// Put adds an entry to the map with the provided key and value.
func (m ConcurrentMap[T]) Put(key string, value T) {
	shard := m.getShard(key)
	shard.Lock()
	shard.items[key] = value
	shard.Unlock()
}

// Get retrieves a value from the map for a given key returning the value
// and a boolean indicating if the value was present in the map. Get also
// accepts a boolean to indicate if the retrieval of the key should be atomic.
// If true a read lock is acquired and then released after the value has been
// retrieved. If atomic is false, no locks are acquired but the value could
// potentially change during the operation.
func (m ConcurrentMap[T]) Get(key string, atomic bool) (T, bool) {
	shard := m.getShard(key)
	if atomic {
		shard.RLock()
		defer shard.RUnlock()
	}
	val, ok := shard.items[key]
	return val, ok
}

// PutIfAbsent first checks if the given key exists in the map. If the key
// exist the map will not be updated and a bool value of false it returned.
// If the key does not exist, a new entry will be added to the map and a
// bool value of true is returned.
func (m ConcurrentMap[T]) PutIfAbsent(key string, value T) bool {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	if _, ok := shard.items[key]; !ok {
		shard.items[key] = value
		return true
	}
	return false
}

// Remove deletes an element from the map. If the given key doesn't exist
// in the map this operation is a no-op.
func (m ConcurrentMap[T]) Remove(key string) {
	shard := m.getShard(key)
	shard.Lock()
	delete(shard.items, key)
	shard.Unlock()
}

// Pop operates similar to Get but removes the entry from the map upon retrieving it
// if it existed. The function returns the value and a boolean value indicating if the
// key existed in the map.
//
// Note that this method takes an exclusive lock because it potentially performs a delete
// operation. Heavy use of Pop can lead to lock contention and performance issues.
func (m ConcurrentMap[T]) Pop(key string) (T, bool) {
	shard := m.getShard(key)
	shard.Lock()
	val, ok := shard.items[key]
	delete(shard.items, key)
	shard.Unlock()
	return val, ok
}

// Upsert either updates an existing element or inserts a new one using the
// UpsertFunc. The UpsertFunc receives if the element exists, the old value
// and the new value and decides what value should be assigned in the map
// for the given key. IE: the value returned from UpsertFunc is what is
// assigned to the provided key in the map.
//
// WARNING: Because this function acquires a lock the UpsertFunc provided must
// not try to access other keys in the map, or it may lead to a deadlock because
// sync.RWLock is not reentrant.
func (m ConcurrentMap[T]) Upsert(key string, val T, cb UpsertFunc[T]) T {
	shard := m.getShard(key)
	shard.Lock()
	old, ok := shard.items[key]
	res := cb(ok, old, val)
	shard.items[key] = res
	shard.Unlock()
	return res
}

// ContainsKey looks up if the provided key exists in the map.
//
// Returns true to indicate the key does exist otherwise false
func (m ConcurrentMap[T]) ContainsKey(key string) bool {
	shard := m.getShard(key)
	shard.RLock()
	_, ok := shard.items[key]
	shard.RUnlock()
	return ok
}

// Keys returns all the keys in the map
//
// Because this operation is atomic within a shard but not across shards there
// may be keys missing or keys that no longer exists if write operations occur
// on shards that have already had their keys collected.
func (m ConcurrentMap[T]) Keys() []string {
	keys := make([]string, 0)
	for _, shard := range m.shards {
		shard.RLock()
		for key := range shard.items {
			keys = append(keys, key)
		}
		shard.RUnlock()
	}
	return keys
}

// Range ranges over each shard, and each entry stored with the shard
// and passes the key and value to the RangeFunc. Underneath a read lock
// is held for each shard therefore the callback sees a consistent view
// of a shard, but not across shards.
//
// This is the cheapest way to read all entries in the map, but the operation
// is atomic at the shard level only, not the map.
//
// WARNING: Because this function acquires a lock the RangeFunc provided must
// not try to access other keys in the map, or it may lead to a deadlock because
// sync.RWLock is not reentrant.
func (m ConcurrentMap[T]) Range(fn RangeFunc[T]) {
	for idx := range m.shards {
		shard := m.shards[idx]
		shard.RLock()
		for key, value := range shard.items {
			fn(key, value)
		}
		shard.RUnlock()
	}
}

// Iter ranges through all the shards and elements sending the entries to the
// provided channel. Providing a nil channel will cause a panic.
//
// Unlike Range this operation is atomic across all shards as it acquires a read
// lock on all shards before beginning to send entries. However, a slow consumer
// can block write operations and potentially lead to issues. Using a buffered
// channel may help alleviate these concerns, but note that once all the elements
// in the map had been sent to the channel the read locks are released and the map
// data may change. IE: The data in a buffered channel could potentially be out of
// date if the consumer is slow.
//
// Iter will close the channel once it has sent all the elements to the channel.
// If the caller closes the channel prematurely this method will panic.
func (m ConcurrentMap[T]) Iter(ch chan<- Entry[T]) {

	// If the programmer is a dummy and sends a nil channel we are going to crash
	// anyway, might as well do it now.
	if ch == nil {
		panic("cannot send entries to nil channel")
	}

	// Acquire a read lock on all shards first
	for i := 0; i < m.shardCount; i++ {
		m.shards[i].RLock()
	}
	defer func() {
		for i := 0; i < m.shardCount; i++ {
			m.shards[i].RUnlock()
		}
	}()

	// Iterate through each shard and all the entries in the shard and send them
	// to the channel provided.
	for i := 0; i < m.shardCount; i++ {
		shard := m.shards[i]
		for key, val := range shard.items {
			ch <- Entry[T]{
				Key:   key,
				Value: val,
			}
		}
	}
	close(ch)
}

// Size returns the size of the map as the count of the number of entries in
// the map.
//
// The value returned may not be precise due the operations being atomic within
// the shard but not across shards. A read lock is acquired and held with each
// shard until the count of the underlying data is retrieved. If there are writes
// or deletes occurring while Size is executing it may not be precise those changes
// occur in shards already counted.
func (m ConcurrentMap[T]) Size() uint64 {
	size := uint64(0)
	for _, shard := range m.shards {
		shard.RLock()
		size = size + uint64(len(shard.items))
		shard.RUnlock()
	}
	return size
}

// Clear removes all elements from the ConcurrentMap.
//
// This operation is atomic at the shard level. Clear iterates over each shard,
// acquires an exclusive lock, and clears the data in the shard.
func (m ConcurrentMap[T]) Clear() {
	for _, shard := range m.shards {
		shard.Lock()
		shard.items = make(map[string]T)
		shard.Unlock()
	}
}

// ShardStats returns the shard number and count of elements in the shard.
func (m ConcurrentMap[T]) ShardStats() map[int]int {
	stats := make(map[int]int, m.shardCount)
	for i := 0; i < m.shardCount; i++ {
		stats[i] = len(m.shards[i].items)
	}
	return stats
}

func (m ConcurrentMap[T]) getShard(key string) *ConcurrentMapShard[T] {
	return m.shards[uint(fnv32(key))%uint(m.shardCount)]
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}
