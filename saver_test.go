package clkhsaver

import (
	"github.com/DATA-DOG/go-sqlmock"
	"testing"
	"time"
)

func TestPush(t *testing.T) {
	clckHouse, mock := GetMock(t)

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO Test").
		WithArgs("hello").
		WillReturnResult(sqlmock.NewResult(1, 5))
	mock.ExpectCommit()

	go clckHouse.Listener()

	time.Sleep(time.Second * 1)
	for i:=0; i < 5 ; i++ {
		clckHouse.Push(map[string]interface{}{"test": "hello"})
	}
}

func GetMock(t *testing.T) (*ClickHouseSaver, sqlmock.Sqlmock) {
	dbm, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	dumper := &EmergencyDumper{
		CheckInterval:time.Duration(5)*time.Second,
		Options: map[string]interface{}{"Type": "fs", "FS.Dir": "/tmp"},
	}

	clckHouse := New(
		"Test",
		"TestUrl",
		time.Duration(10)*time.Second,
		time.Duration(5)*time.Second,
		5,
		func(err error) {
			t.Log(err)
		},
		dumper,
	)

	err = clckHouse.initialize(dbm)
	if err != nil {
		t.Fatalf("can't open connection: %s", err)
	}

	return clckHouse, mock
}