package datastore

import (
	"reflect"
	"sort"
	"sync"
)

// Collection is a RWMutex-managed map containing a type that embeds Document.
// The fields Type, Items, and CurrentIndex are public for serialization
// purposes but you should NEVER modify these directly. Use the methods instead.
type Collection struct {
	// Type indicates the reflected type of the Documents stored in this
	// collection. DO NOT MODIFY.
	Type         string

	// Items holds the map of Documents in this Collection. DO NOT MODIFY.
	Items        map[uint64]Document

	// CurrentIndex holds the autoincrement value for this Collection. DO NOT MODIFY.
	CurrentIndex uint64

	list         []uint64
	mutex        sync.RWMutex
}

func (c *Collection) SetType(document Document) error {
	kind := reflect.TypeOf(document).String()
	switch c.Type {
	case "": // Type isn't set yet, so set it to the current type
		c.Type = kind
		return nil
	case kind: // Type is set to the same thing here, so do nothing
		return nil
	// Type is set to something different, return an error
	default:
		return ErrInvalidType
	}
}

// Upsert inserts or updates a Document in the collection.
func (c *Collection) Upsert(document Document) error {
	if err := c.SetType(document); err != nil {
		return err
	}

	c.mutex.Lock()

	if document.ID() == 0 {
		c.CurrentIndex += 1
		document.SetID(c.CurrentIndex)
		c.list = append(c.list, document.ID())
	}

	c.Items[document.ID()] = document
	c.mutex.Unlock()
	return nil
}

// DeleteKey removes the indicated key from the Collection, or no-ops if the key
// is not present.
func (c *Collection) DeleteKey(key uint64) {
	c.mutex.Lock()
	delete(c.Items, key)
	deleteKeyFromList(&c.list, key)
	c.mutex.Unlock()
}

// Delete removes the Document from the Collection and sets the ID to zero.
func (c *Collection) Delete(document Document) {
	if document.ID() == 0 {
		return
	}
	c.DeleteKey(document.ID())
	document.SetID(0)
}

// Find a Document by key. This is useful for "foreign key" type relationships
// where you do not want to create a cyclical reference between objects, or a
// fast lookup by ID if you already know it. Returns nil if the key is not found
// in the Collection.
func (c *Collection) FindKey(key uint64) Document {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	item, ok := c.Items[key]
	if !ok {
		return nil
	}
	return item
}

// Filter is a lookup-style function that returns a list of Documents that
// satisfy the filter. The Collection is scanned in ascending order. All
// Documents satisfying the callback will be returned. If none are found the
// list will be empty. This function always enumerates the entire Collection
// (i.e. table scan).
func (c *Collection) FindAll(finder func(Document) bool) []Document {
	found := []Document{}
	c.mutex.RLock()

	for _, key := range c.List() {
		if finder(c.Items[key]) {
			found = append(found, c.Items[key])
		}
	}

	c.mutex.RUnlock()
	return found
}

// FindOne is a lookup-style function that returns the first Document that
// satisfies the callback. The Collection is scanned in ascending order. FindOne
// enumerates the entire Collection (i.e. table scan) until a match is found, or
// returns nil if there is no match.
func (c *Collection) FindOne(finder func(Document) bool) Document {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	for _, key := range c.List() {
		if finder(c.Items[key]) {
			return c.Items[key]
		}
	}
	return nil
}

// List returns a sorted list of keys (in ascending order) for all Documents
// currently held in the Collection.
func (c *Collection) List() []uint64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.list
}

// generateList is an internal call that rebuilds the list of keys after
// restoring a Datastore from disk. It should not need to be called otherwise.
func (c *Collection) generateList() {
	c.mutex.Lock()
	c.list = []uint64{}
	for _, item := range c.Items {
		c.list = append(c.list, item.ID())
	}
	sort.Sort(UIntSlice(c.list))
	c.mutex.Unlock()
}
