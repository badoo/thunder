package vhash

import (
	"sync"
)

type Vhash struct {
	mu    sync.Mutex
	table map[uint64]*Item
}

// Values that go into Vhash need to satisfy this interface.
type Value interface {
}

type Item struct {
	Key   uint64
	Value Value
}

type entry struct {
	key   uint64
	value Value
}

func NewVhash() *Vhash {
	return &Vhash{
		table: make(map[uint64]*Item),
	}
}

func (vh *Vhash) Lock() {
	vh.mu.Lock()
}

func (vh *Vhash) Unlock() {
	vh.mu.Unlock()
}

func (vh *Vhash) Get(key uint64) (v Value, ok bool) {
	element := vh.table[key]
	if element == nil {
		return nil, false
	}
	return element.Value, true
}

func (vh *Vhash) Set(key uint64, value Value) {
	if element := vh.table[key]; element != nil {
		vh.updateInplace(element, value)
	} else {
		vh.addNew(key, value)
	}
}

func (vh *Vhash) Delete(key uint64) bool {
	element := vh.table[key]
	if element == nil {
		return false
	}

	delete(vh.table, key)

	return true
}

func (vh *Vhash) Clear() {
	vh.Lock()
	defer vh.Unlock()

	vh.table = make(map[uint64]*Item)
}

func (vh *Vhash) updateInplace(element *Item, value Value) {
	element.Value = value
}

func (vh *Vhash) addNew(key uint64, value Value) {
	newEntry := &Item{key, value}
	vh.table[key] = newEntry
}
