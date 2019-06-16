package datastore

// UintSlice implements the Sort interface for a slice of uint64. Rather than
// declare your own variables using this type you only need to wrap the []uint64
// during the sort call.
type UIntSlice []uint64

func (u UIntSlice) Len() int           { return len(u) }
func (u UIntSlice) Less(i, j int) bool { return u[i] < u[j] }
func (u UIntSlice) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }

func binarySearchList(list *[]uint64, key uint64) int {
	low, mid, high := 0, 0, len(*list)-1

	for low <= high {
		mid = low + ((high - low) / 2)
		if key < (*list)[mid] {
			high = mid - 1
		} else if key > (*list)[mid] {
			low = mid + 1
		} else {
			return mid
		}
	}

	return -1
}

func scanList(list *[]uint64, key uint64) int {
	for i, val := range *list {
		if val == key {
			return i
		}
	}

	return -1
}

// deleteKeyFromList searches for and removes a uint64 from a list of uint64's.
func deleteKeyFromList(list *[]uint64, key uint64) {
	// TODO This is *really* slow if you're deleting a lot of items. Potentially
	//  it's if you are deleting more than one item at at time because we have
	//  to rewrite the list after the item that's deleted.
	idx := binarySearchList(list, key)

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
	copy((*list)[idx:], (*list)[idx+1:])
	*list = (*list)[:len(*list)-1]
	//*list = append((*list)[0:idx], (*list)[idx+1:]...)
}
