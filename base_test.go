package sorm

import (
	"database/sql"
	"reflect"
	"testing"
	"time"
)

type testEntity struct {
	AbstractEntity

	TestId      int       `db:"test_id" primary:"true"`
	Name        string    `db:"name"`
	TimeAdded   time.Time `db:"time_added"`
	TimeCurrent time.Time `db:"time_current"`
}

var (
	db = &mockDb{}
)

type mockResult struct{}

func (mr mockResult) LastInsertId() (int64, error) {
	return 1, nil
}

func (dr mockResult) RowsAffected() (int64, error) {
	return 1, nil
}

type mockDb struct{}

type drvRows struct{}

func (mdb *mockDb) Query(query string, args ...interface{}) (*sql.Rows, error) {
	rows := &sql.Rows{}
	rows.Close()
	return rows, nil
}

func (mdb *mockDb) Exec(query string, args ...interface{}) (sql.Result, error) {
	return mockResult{}, nil
}

func TestNewEntity(t *testing.T) {
	test := &testEntity{}
	test.Name = "Jake"
	test.TimeAdded = time.Now()

	if test.TestId != 0 {
		t.Fatalf("Expected testEntity Id to be `%d`, got `%d`", 0, test.TestId)
	}

	Save(db, test)

	if test.TestId == 0 {
		t.Fatalf("Expected testEntity Id to not be `%d`, got `%d`", 0, test.TestId)
	}
}

func TestLoadUpdateEntity(t *testing.T) {
	test := &testEntity{}
	primaryKey := 1
	// TODO fix the test (need proper rows mocking)
	return
	LoadEntity(db, test, "TestId", primaryKey)

	if test.TestId != primaryKey {
		t.Fatalf("Expected testEntity Id to be `%d`, got `%d`", primaryKey, test.TestId)
	}

	v := reflect.ValueOf(test).Elem()
	changedFieldNamesCount := len(getChangedFieldNames(v))
	expectedChangedFields := 0
	if changedFieldNamesCount != expectedChangedFields {
		t.Fatalf("Expected testEntity chaned fields to be `%d`, got `%d`", expectedChangedFields, changedFieldNamesCount)
	}

	test.TimeCurrent = time.Now()

	expectedChangedFields++

	changedFieldNamesCount = len(getChangedFieldNames(v))
	if changedFieldNamesCount != expectedChangedFields {
		t.Fatalf("Expected testEntity chaned fields to be `%d`, got `%d`", expectedChangedFields, changedFieldNamesCount)
	}

	test.Name = "Majkl"

	expectedChangedFields++

	changedFieldNamesCount = len(getChangedFieldNames(v))
	if changedFieldNamesCount != expectedChangedFields {
		t.Fatalf("Expected testEntity chaned fields to be `%d`, got `%d`", expectedChangedFields, changedFieldNamesCount)
	}

	Save(db, test)

	changedFieldNamesCount = len(getChangedFieldNames(v))
	if changedFieldNamesCount != 0 {
		t.Fatalf("Expected testEntity chaned fields to be `%d`, got `%d`", 0, changedFieldNamesCount)
	}
}

type extendedEntity struct {
	AbstractEntity

	Id          int         `db:"id" primary:"true"`
	TestPtr     *testEntity `db:"test_ptr_id"`
	TimeAdded   time.Time   `db:"time_added"`
	TimeCurrent time.Time   `db:"time_current"`
}

func TestSpecialEntity(t *testing.T) {
	entity := &extendedEntity{
		TestPtr: &testEntity{
			TestId: 60,
			Name:   "Majkl",
		},
		TimeAdded: time.Now(),
	}

	Save(db, entity)
}
