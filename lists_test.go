package datastore

import (
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func TestDeleteKeyFromList(t *testing.T) {
	t.Run("delete first item", func(t *testing.T) {
		list := []uint64{1, 2, 3, 4}
		expected := []uint64{2, 3, 4}
		deleteKeyFromList(&list, 1)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete last item", func(t *testing.T) {
		list := []uint64{1, 2, 3, 4}
		expected := []uint64{1, 2, 4}
		deleteKeyFromList(&list, 3)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete second item", func(t *testing.T) {
		list := []uint64{1, 2, 3, 4}
		expected := []uint64{1, 3, 4}
		deleteKeyFromList(&list, 2)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete third item", func(t *testing.T) {
		list := []uint64{1, 2, 3, 4}
		expected := []uint64{1, 2, 3}
		deleteKeyFromList(&list, 4)
		if !reflect.DeepEqual(list, expected) {
			t.Errorf("Expected %#v, found %#v", expected, list)
		}
	})

	t.Run("delete missing item", func(t *testing.T) {
		list := []uint64{1, 2, 3, 4}
		expected := []uint64{1, 2, 3, 4}
		deleteKeyFromList(&list, 5)
		if ! reflect.DeepEqual(list, expected) {
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

var result []uint64

func SetupLists(size, ratio int) (list, search []uint64) {
	var seed int64 = 543290877543

	// ratio will determine how much of the list we are deleting.
	// For example size factor 4 means we delete 25% of the items in the
	// list. 32 is approximately 3% deletion rate.
	rng := rand.New(rand.NewSource(seed))
	list = make([]uint64, size *ratio)
	for i := 0; i < size *ratio; i++ {
		list[i] = rng.Uint64()
	}
	sort.Sort(UIntSlice(list))

	// Reinitialize the random source with the same seed so we get (some of) the same numbers
	rng = rand.New(rand.NewSource(seed))
	search = make([]uint64, size)
	for i := 0; i < size; i++ {
		search[i] = rng.Uint64()
	}

	return
}

// Make sure the test has side effects so the compiler does not optimize it away
var t = 0

func TestSearchScanList(t *testing.T) {
	list := []uint64{2, 4, 6, 7, 8, 9, 13, 16, 17, 19, 20, 21, 22, 23, 24, 25,26, 27, 28, 29, 30, 32, 33, 34, 36, 37, 38, 39, 40, 41, 44, 45, 47, 48, 50, 51, 53, 55, 57, 60, 61, 62, 63, 64, 65, 80, 81, 82, 83, 84, 85}

	type testCase struct {
		Input uint64
		Expected int
	}

	cases := []testCase {
		{ 1, -1},
		{ 2, 0},
		{ 13, 6},
		{ 15, -1},
		{ 23, 13},
		{ 40, 28},
		{ 85, 50},
	}

	for _, c := range cases {
		actual := scanList(&list, c.Input)
		if actual != c.Expected {
			t.Errorf("Expected %d, found %d", c.Expected, actual)
		}
	}

	for _, c := range cases {
		actual := binarySearchList(&list, c.Input)
		if actual != c.Expected {
			t.Errorf("Expected %d, found %d", c.Expected, actual)
		}
	}
}

func BenchmarkScanList(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	list, search := SetupLists(b.N, 32)
	b.StartTimer()

	for _, s := range search {
		t = scanList(&list, s)
	}
}

func BenchmarkBinarySearchList(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	list, search := SetupLists(b.N, 32)
	b.StartTimer()

	for _, s := range search {
		t = binarySearchList(&list, s)
	}
}

func BenchmarkDeleteKeyFromList(b *testing.B) {
	b.ReportAllocs()
	b.StopTimer()
	list, search := SetupLists(b.N, 32)
	b.StartTimer()

	for i := 0; i < b.N; i++ {
		deleteKeyFromList(&list, search[i])
	}

	result = list
}
