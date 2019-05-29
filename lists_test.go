package datastore

import (
	"reflect"
	"sort"
	"testing"
)

func TestDeleteKeyFromList(t *testing.T) {
	t.Run("delete first item", func(t *testing.T) {
		list := []uint64{1, 2, 4, 3}
		expected := []uint64{2, 4, 3}
		deleteKeyFromList(&list, 1)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete last item", func(t *testing.T) {
		list := []uint64{1, 2, 4, 3}
		expected := []uint64{1, 2, 4}
		deleteKeyFromList(&list, 3)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete second item", func(t *testing.T) {
		list := []uint64{1, 2, 4, 3}
		expected := []uint64{1, 4, 3}
		deleteKeyFromList(&list, 2)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete third item", func(t *testing.T) {
		list := []uint64{1, 2, 4, 3}
		expected := []uint64{1, 2, 3}
		deleteKeyFromList(&list, 4)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})
}

func TestSortUIntSlice(t *testing.T) {
	list := []uint64{100, 7, 18, 3}
	expected := []uint64{3, 7, 18, 100}

	sort.Sort(UIntSlice(list))
	if !reflect.DeepEqual(list, expected) {
		t.Errorf("Expected %#v, found %#v", expected, list)
	}
}
