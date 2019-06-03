package datastore_test

import (
	"reflect"
	"testing"

	"git.stormbase.io/cbednarski/datastore"
	"strings"
)

func TestCollection_Upsert(t *testing.T) {
	ds := datastore.New()
	cakes := ds.In("cake")

	chocolate := &NameDocument{
		Name: "chocolate cake",
	}
	number := &NumberDocument{
		Number: 100,
	}

	if err := cakes.Upsert(chocolate); err != nil {
		t.Error(err)
	}
	if err := cakes.Upsert(number); err != datastore.ErrInvalidType {
		t.Errorf("Expected %s, found %s", datastore.ErrInvalidType, err)
	}

	id := chocolate.ID()
	if chocolate.ID() != 1 {
		t.Errorf("First document should be 1, found %d", chocolate.ID())
	}

	expectedList := []uint64{1}
	if !reflect.DeepEqual(cakes.List(), expectedList) {
		t.Errorf("Expected %#v, found %#v", expectedList, cakes.List())
	}

	if err := cakes.Upsert(chocolate); err != nil {
		t.Error(err)
	}

	if chocolate.ID() != id {
		t.Errorf("Document ID changed after second upsert")
	}

	if !reflect.DeepEqual(cakes.List(), expectedList) {
		t.Errorf("Expected %#v, found %#v", expectedList, cakes.List())
	}
}

func TestCollection_Delete(t *testing.T) {
	ds := datastore.New()
	cakes := ds.In("cake")

	chocolate := &NameDocument{
		Name: "chocolate cake",
	}
	vanilla := &NameDocument{
		Name: "vanilla cake",
	}

	// should be a no-op
	cakes.Delete(vanilla)

	if err := cakes.Upsert(chocolate); err != nil {
		t.Error(err)
	}
	if err := cakes.Upsert(vanilla); err != nil {

	}

	if chocolate.ID() != 1 {
		t.Errorf("Expected ID 1 for chocolate")
	}

	cakes.Delete(chocolate)

	if chocolate.ID() != 0 {
		t.Errorf("Expected ID 0 after deletion")
	}

	expected := []uint64{2}
	if !reflect.DeepEqual(cakes.List(), expected) {
		t.Errorf("Expected %#v, found %#v", expected, cakes.List())
	}
}

func TestCollection_FindKey(t *testing.T) {
	cakeTests := []string{
		"chocolate",
		"vanilla",
		"strawberry",
	}

	ds := datastore.New()
	cakes := ds.In("cakes")

	for _, cake := range cakeTests {
		err := cakes.Upsert(&NameDocument{
			Name: cake,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	nope := cakes.FindKey(60)
	if nope != nil {
		t.Errorf("Expected nil")
	}

	var expectedIdx uint64 = 3
	strawberry := cakes.FindKey(expectedIdx)
	if strawberry.ID() != expectedIdx {
		t.Errorf("Expected %d, found %d", expectedIdx, strawberry.ID())
	}

	strawberryCake, ok := strawberry.(*NameDocument)
	if !ok {
		t.Fatal("Expected *NameDocument type")
	}

	expectedName := "strawberry"
	if strawberryCake.Name != expectedName {
		t.Errorf("Expected %s, found %s", expectedName, strawberryCake.Name)
	}
}

func TestCollection_FindAll(t *testing.T) {
	items := []string{
		"chocolate cookies",
		"lemon cake",
		"chocolate cake",
		"strawberry shortcake",
		"chocolate caramels",
		"icecream",
	}

	ds := datastore.New()
	desserts := ds.In("desserts")

	for _, item := range items {
		err := desserts.Upsert(&NameDocument{
			Name: item,
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	onADiet := desserts.FindAll(func(d datastore.Document) bool {
		return false
	})

	if len(onADiet) != 0 {
		t.Errorf("Expected no results")
	}

	yumyum := desserts.FindAll(func(d datastore.Document) bool {
		return true
	})

	if len(yumyum) != len(items) {
		t.Errorf("Expected ALL the results")
	}

	chocolates := desserts.FindAll(func(d datastore.Document) bool {
		dessert, ok := d.(*NameDocument)
		if ok && strings.Contains(dessert.Name, "chocolate") {
			return true
		}
		return false
	})

	if len(chocolates) != 3 {
		t.Errorf("Expected 3 chocolate desserts")
	}

	expectedNames := []string{
		items[0],
		items[2],
		items[4],
	}

	for key, name := range expectedNames {
		if choco, ok := chocolates[key].(*NameDocument); ok {
			if choco.Name != name {
				t.Errorf("Expected %s, found %s", name, choco.Name)
			}
		} else {
			t.Errorf("%#v should be type *NameDocument", choco)
		}
	}
}

func TestCollection_FindOne(t *testing.T) {
	ds := datastore.New()
	cookies := ds.In("items")

	bestCookie := "chocolate chip"
	chocochip := &NameDocument{
		Name: bestCookie,
	}

	if err := cookies.Upsert(chocochip); err != nil {
		t.Error(err)
	}

	noCookie := cookies.FindOne(func(d datastore.Document) bool {
		return false
	})

	if noCookie != nil {
		t.Errorf("Expected nil, found %#v", noCookie)
	}

	yesCookie := cookies.FindOne(func(d datastore.Document) bool {
		cookie, ok := d.(*NameDocument)
		if ok && cookie.Name == bestCookie {
			return true
		}
		return false
	})

	if yesCookie == nil {
		t.Fatal("Expected non-nil value")
	}

	chocoChip2, ok := yesCookie.(*NameDocument)
	if !ok {
		t.Fatal("Expected *NameDocument type")
	}

	if chocoChip2.Name != bestCookie {
		t.Errorf("Expected %s, found %s", bestCookie, chocoChip2.Name)
	}
}
