# Concurrent Map

_This project was originally forked from orcaman/concurrent-map. Much of the ideas, API, and code was inspired from that project. I've taken the ideas from that project and added support for generics. I've also made a few API and design changes but the core concepts are largely the same._

The built-in `map` type in Go doesn't support concurrent writes. This project aims a providing a high-performance solution to this by sharding the map to reduce lock contention.

Prior to Go 1.9, there was no concurrent map implementation in the stdlib. In Go 1.9, sync.Map was introduced. The new sync.Map has a few key differences from this map. The stdlib sync.Map is designed for append-only scenarios. So if you want to use the map for something more like in-memory db, you might benefit from using Concurrent Map.

## Usage

Installation

`go get github.com/jkratz55/concurrent-map`

Basic Example

```go
package main

import (
	"fmt"

	"github.com/jkratz55/concurrent-map"
)

func main() {

	// Create a new map with 32 shards. 32 is the default shards and we can
	// use concurrentmap.DefaultShards here is we want
	m := concurrentmap.New[string](32)

	// Adds items to the map
	m.Put("foo", "bar")
	m.Put("taco", "salad")

	// Only add items if they aren't already in the map
	if ok := m.PutIfAbsent("foo", "barrrrrr"); ok {
		fmt.Println("added foo to the map!")
	}

	// Get items from the map atomically
	v, ok := m.Get("foo", true)
	if !ok {
		fmt.Println("ohhh no the key isn't in the map")
	}
	fmt.Println(v)

	// Get items from the map non-atomic (doesn't use a read lock)
	v, ok = m.Get("foo", false)
	if !ok {
		fmt.Println("ohhh no the key isn't in the map")
	}
	fmt.Println(v)

	// Remove item from map
	m.Remove("taco")
}
```