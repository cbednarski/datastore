//go:generate go test -run TestGenerateDatastore

package datastore_test

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"git.stormbase.io/cbednarski/datastore"
)

const (
	ExpectedName    = "testeroonie"
	DatastoreFormat = "datastore-test-format"
	Names           = "names"
)

func TestIn(t *testing.T) {
	ds := datastore.New()
	collection := ds.In("cake")

	if collection.Type != "" {
		t.Errorf("Expected empty string, found %#v", collection.Type)
	}
}

func TestInit(t *testing.T) {
	ds := datastore.New()
	doc := &NameDocument{}
	docType := reflect.TypeOf(&NameDocument{}).String()

	collection, err := ds.Init("cake", doc)
	if err != nil {
		t.Errorf("Expected no error, found %s", err)
	}
	if collection.Type != docType {
		t.Errorf("Expected collection type %q, found %q", docType, collection.Type)
	}

	numdoc := &NumberDocument{}
	_, err = ds.Init("cake", numdoc)
	if err != datastore.ErrInvalidType {
		t.Errorf("Expected %q, found %q", datastore.ErrInvalidType, err)
	}
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

	nameDoc := &NameDocument{
		Name: ExpectedName,
	}

	if err := ds.In(Names).Upsert(nameDoc); err != nil {
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

	document := ds.In(Names).FindKey(1)

	if document == nil {
		t.Fatal("document is nil")
	}

	dummy, ok := document.(*NameDocument)
	if !ok {
		t.Fatal("document is not a *NameDocument")
	}

	if dummy.Name != ExpectedName {
		t.Errorf("Expected %q, found %q", ExpectedName, dummy.Name)
	}
}

func TestSignature(t *testing.T) {
	signature := "cakes"
	expectedSignature := "datastore:cakes"

	if datastore.Signature(signature) != expectedSignature {
		t.Errorf("Expected %q, found %q", expectedSignature, datastore.Signature(DatastoreFormat))
	}
}

func TestSignatureOnCreate(t *testing.T) {
		testFile := filepath.Join(os.TempDir(), "sample-datastore"+datastore.Extension)
		defer os.RemoveAll(testFile)

		signature := "throwaway"

		ds, err := datastore.Create(testFile, signature)
		if err != nil {
			t.Fatal(err)
		}

		expectedSignature := datastore.Signature(signature)
		if ds.Signature() != expectedSignature {
			t.Errorf("Expected %q, found %q", expectedSignature, ds.Signature())
		}
	}

func TestSignatureOnOpen(t *testing.T) {
		expectedSignature := "datastore:"+DatastoreFormat

		ds, err := datastore.Open(filepath.Join("testdata", "datastore"), DatastoreFormat)
		if err != nil {
			t.Fatal(err)
		}

		if ds.Signature() != expectedSignature {
			t.Errorf("Expected %q, found %q", expectedSignature, ds.Signature())
		}
}

func TestReadSignatureOnOpenError(t *testing.T) {
	_, err := datastore.Open(filepath.Join("testdata", "datastore"), "candy")
	if err != datastore.ErrInvalidSignature {
		t.Errorf("Expected %q, found %q", datastore.ErrInvalidSignature, err)
	}
}

func TestReadSignature(t *testing.T) {
	expectedSignature := "datastore:"+DatastoreFormat

	signature, err := datastore.ReadSignature(filepath.Join("testdata", "datastore"))
	if err != nil {
		t.Fatal(err)
	}

	if signature != expectedSignature {
		t.Errorf("Expected %q, found %q", expectedSignature, signature)
	}
}
