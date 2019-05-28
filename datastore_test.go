//go:generate go test -run TestGenerateDatastore

package datastore

import (
	"encoding/gob"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

const (
	ExpectedName    = "testeroonie"
	DatastoreFormat = "datastore-test-format"
)

type DummyDocument struct {
	Identifier uint64
	Name       string
}

func (t *DummyDocument) ID() uint64 {
	return t.Identifier
}

func (t *DummyDocument) SetID(i uint64) {
	t.Identifier = i
}

func init() {
	gob.Register(&DummyDocument{})
}

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

func TestCreateDatastore(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "siphon")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempdir)

	datapath := filepath.Join(tempdir, Extension)
	datastore, err := Create(datapath, DatastoreFormat)
	if err != nil {
		t.Fatal(err)
	}

	if datastore.path != datapath {
		t.Errorf("Expected %s, found %s", datapath, datastore.path)
	}
}

func TestGenerateDatastore(t *testing.T) {
	search := "-test.run=TestGenerateDatastore"

	found := false
	for _, val := range os.Args {
		if val == search {
			found = true
		}
	}

	// Run this test by name when you need to re-generate the datastore fixture:
	//
	//	go test ./datastore/ -run TestGenerateDatastore
	if !found {
		t.SkipNow()
	}

	path := filepath.Join("testdata", "datastore")
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}

	ds, err := Create(path, DatastoreFormat)
	if err != nil {
		t.Fatal(err)
	}

	dummy := &DummyDocument{
		Name: ExpectedName,
	}

	cname := CName(dummy)

	if err := ds.In(cname).Upsert(dummy); err != nil {
		t.Fatal(err)
	}

	if err := ds.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDatastore(t *testing.T) {
	ds, err := Open(filepath.Join("testdata", "datastore"), DatastoreFormat)
	if err != nil {
		t.Fatal(err)
	}

	cname := CName(&DummyDocument{})

	document := ds.In(cname).FindKey(1)

	if document == nil {
		t.Fatal("document is nil")
	}

	dummy, ok := document.(*DummyDocument)
	if !ok {
		t.Fatal("document is not a *DummyDocument")
	}

	if dummy.Name != ExpectedName {
		t.Errorf("Expected %q, found %q", ExpectedName, dummy.Name)
	}
}
