package main

import (
	"database/sql"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/doug-martin/goqu/v9"
	"github.com/rs/xid"
)

type Object struct {
	ID   string
	Data string
}

func downloadObject(url string) (*Object, error) {
	log.Println("downloading", url)

	time.Sleep(DEFAULT_DELAY)

	res, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	data, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	obj := Object{
		ID:   xid.New().String(),
		Data: string(data),
	}

	log.Println("downloaded", obj.ID)
	return &obj, nil
}

func saveObject(db *sql.DB, obj *Object) error {
	log.Println("saving", obj.ID)

	stmt := PG.Insert(OBJECTS_TBL).Rows(goqu.Record{"id": obj.ID, "data": obj.Data})
	q, args, err := stmt.ToSQL()
	if err != nil {
		return err
	}

	if _, err := db.Exec(q, args...); err != nil {
		return err
	}

	log.Println("saved", obj.ID)
	return nil
}
