//go:generate go test -run TestGenerateDatastore

package datastore_test

import (
	"encoding/gob"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"git.stormbase.io/cbednarski/datastore"
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

func TestCreateDatastore(t *testing.T) {
	tempdir, err := ioutil.TempDir("", "siphon")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempdir)

	datapath := filepath.Join(tempdir, datastore.Extension)
	datastore, err := datastore.Create(datapath, DatastoreFormat)
	if err != nil {
		t.Fatal(err)
	}

	if datastore.Path() != datapath {
		t.Errorf("Expected %s, found %s", datapath, datastore.Path())
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

	ds, err := datastore.Create(path, DatastoreFormat)
	if err != nil {
		t.Fatal(err)
	}

	dummy := &DummyDocument{
		Name: ExpectedName,
	}

	cname := datastore.CName(dummy)

	if err := ds.In(cname).Upsert(dummy); err != nil {
		t.Fatal(err)
	}

	if err := ds.Flush(); err != nil {
		t.Fatal(err)
	}
}

func TestLoadDatastore(t *testing.T) {
	ds, err := datastore.Open(filepath.Join("testdata", "datastore"), DatastoreFormat)
	if err != nil {
		t.Fatal(err)
	}

	cname := datastore.CName(&DummyDocument{})

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

func TestSignature(t *testing.T) {
	expectedSignature := "datastore:"+DatastoreFormat

	if datastore.Signature(DatastoreFormat) != expectedSignature {
		t.Errorf("Expected %q, found %q", expectedSignature, datastore.Signature(DatastoreFormat))
	}
}
