package datastore

// UintSlice implements the Sort interface for a slice of uint64. Rather than
// declare your own variables using this type you only need to wrap the []uint64
// during the sort call.
type UIntSlice []uint64

func (u UIntSlice) Len() int           { return len(u) }
func (u UIntSlice) Less(i, j int) bool { return u[i] < u[j] }
func (u UIntSlice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }

// deleteKeyFromList searches for and removes a uint64 from a list of uint64's.
func deleteKeyFromList(list *[]uint64, key uint64) {
	// TODO replace this with a binary search when the list grows so we get
	//  O(log n) instead of O(n). For smaller datasets it doesn't matter.
	idx := -1
	for i, val := range *list {
		if val == key {
			idx = i
			break
		}
	}

	// not found, exit
	if idx < 0 {
		return
	}
	// First item
	if idx == 0 {
		*list = (*list)[1:]
		return
	}
	// Last item
	if idx == len(*list)-1 {
		*list = (*list)[0:idx]
		return
	}
	// Middle item
	*list = append((*list)[0:idx], (*list)[idx+1:]...)
}
