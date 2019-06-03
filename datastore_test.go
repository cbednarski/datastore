//go:generate go test -run TestGenerateDatastore

package datastore_test

import (
	"encoding/gob"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"git.stormbase.io/cbednarski/datastore"
	"strings"
)

const (
	ExpectedName      = "testeroonie"
	TestdataSignature = "datastore-test-format"
	Names             = "names"
)

var TestdataDatastore = filepath.Join("testdata", "datastore")
var TestdataInvalid = filepath.Join("testdata", "invalid.datastore")
var TestdataReadonly = filepath.Join("testdata", "readonly")

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

	t.Run("working datastore", func(t *testing.T) {
		if err := os.RemoveAll(TestdataDatastore); err != nil {
			t.Fatal(err)
		}

		ds, err := datastore.Create(TestdataDatastore, TestdataSignature)
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
	})

	// Invalid datastore contains InvalidDocuments which are not registered
	// with Gob, so they will fail to decode.
	t.Run("invalid datastore", func(t *testing.T) {
		if err := os.RemoveAll(TestdataInvalid); err != nil {
			t.Fatal(err)
		}

		ds, err := datastore.Create(TestdataInvalid, TestdataSignature)
		if err != nil {
			t.Fatal(err)
		}

		invalidDoc := &InvalidDocument{
			Name: "so invalid",
		}

		if err := ds.In("invalid").Upsert(invalidDoc); err != nil {
			t.Fatal(err)
		}

		gob.Register(invalidDoc)

		if err := ds.Flush(); err != nil {
			t.Fatal(err)
		}
	})
}

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
	ds, err := datastore.Create(datapath, TestdataSignature)
	if err != nil {
		t.Fatal(err)
	}

	if ds.Path() != datapath {
		t.Errorf("Expected %s, found %s", datapath, ds.Path())
	}
}

func TestCreateDatastoreDoesNotExist(t *testing.T) {
	_, err := datastore.Create(filepath.Join("doesnotexist", "filename"), "sig")
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Expected error, folder does not exist, found %#v", err)
	}
}

func TestCreateDatastoreNoPermission(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Test must be run as a non-root user")
	}

	readonlyPath := "/root/test.datastore"
	if runtime.GOOS == "windows" {
		readonlyPath = `C:\Windows\test.datastore`
	}

	_, err := datastore.Create(readonlyPath, "sig")
	if err == nil {
		t.Error("Expected error, no write permissions")
	}
}

func TestOpenDatastore(t *testing.T) {
	_, err := datastore.Open(TestdataDatastore, TestdataSignature)
	if err != nil {
		t.Error(err)
	}
}

func TestOpenDatastoreDoesNotExist(t *testing.T) {
	_, err := datastore.Open("doesnotexist", "sig")
	if err == nil || !os.IsNotExist(err) {
		t.Errorf("Expected error, folder does not exist, found %#v", err)
	}
}

func TestOpenDatastoreNoPermission(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("Test must be run as a non-root user")
	}

	readonlyPath := "/root/test.datastore"
	if runtime.GOOS == "windows" {
		t.Skip("Test needs additional input for Windows (bug)")
		// I don't know a path on Windows that cannot be read by normal users so
		// I don't know how to make this test fail on purpose.
		readonlyPath = `C:\Windows\test.datastore`
	}

	_, err := datastore.Open(readonlyPath, "sig")
	if err == nil {
		t.Error("Expected error, no read permissions")
	}
}

func TestOpenDatastoreInvalidGzip(t *testing.T) {
	_, err := datastore.Open(TestdataReadonly, TestdataSignature)
	if err == nil || err.Error() != "gzip: invalid header" {
		t.Errorf("Expected error, invalid gzip, found %#v", err)
	}
}

func TestOpenDatastoreInvalidGob(t *testing.T) {
	_, err := datastore.Open(TestdataInvalid, TestdataSignature)
	if err == nil || !strings.Contains(err.Error(), "name not registered for interface") {
		t.Errorf("Expected error, 'name not registered for interface', found %#v", err)
	}
}

func TestOpenOrCreate(t *testing.T) {
	_, err := datastore.OpenOrCreate(TestdataDatastore, TestdataSignature)
	if err != nil {
		t.Error(err)
	}

	tempdir, err := ioutil.TempDir("", "siphon")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempdir)

	datapath := filepath.Join(tempdir, "temp"+datastore.Extension)
	ds, err := datastore.OpenOrCreate(datapath, TestdataSignature)
	if err != nil {
		t.Fatal(err)
	}

	if ds.Path() != datapath {
		t.Errorf("Expected %s, found %s", datapath, ds.Path())
	}
}

func TestLoadDatastore(t *testing.T) {
	ds, err := datastore.Open(TestdataDatastore, TestdataSignature)
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
		t.Errorf("Expected %q, found %q", expectedSignature, datastore.Signature(TestdataSignature))
	}
}

func TestSignatureOnCreate(t *testing.T) {
	// TODO This test completely duplicates some other Open and Create tests;
	//  it would be nice to remove the duplication
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
		expectedSignature := "datastore:"+ TestdataSignature

		ds, err := datastore.Open(TestdataDatastore, TestdataSignature)
		if err != nil {
			t.Fatal(err)
		}

		if ds.Signature() != expectedSignature {
			t.Errorf("Expected %q, found %q", expectedSignature, ds.Signature())
		}
}

func TestReadSignatureOnOpenError(t *testing.T) {
	_, err := datastore.Open(TestdataDatastore, "candy")
	if err != datastore.ErrInvalidSignature {
		t.Errorf("Expected %q, found %q", datastore.ErrInvalidSignature, err)
	}
}

func TestReadSignature(t *testing.T) {
	expectedSignature := "datastore:"+ TestdataSignature

	signature, err := datastore.ReadSignature(TestdataDatastore)
	if err != nil {
		t.Fatal(err)
	}

	if signature != expectedSignature {
		t.Errorf("Expected %q, found %q", expectedSignature, signature)
	}

	_, err = datastore.ReadSignature("doesnotexist")
	if err == nil {
		t.Error("Expected error, file does not exist")
	} else if !os.IsNotExist(err) {
		t.Errorf("Expected os.IsNotExist, found %s", err)
	}

	_, err = datastore.ReadSignature("/root/datastore")
	if err == nil {
		t.Errorf("Expected error, can't read this file")
	}

	_, err = datastore.ReadSignature(TestdataReadonly)
	if err == nil {
		t.Errorf("Expected error, no gzip data")
	}
}
