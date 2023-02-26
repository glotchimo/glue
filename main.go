package main

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"github.com/doug-martin/goqu/v9"
	_ "github.com/lib/pq"
	"github.com/rs/xid"
)

type Status string

const (
	BASE_URL  = "https://httpbin.org"
	DELAY_URL = BASE_URL + "/delay/%d"

	DSN         = "postgresql://localhost:5432/bin?sslmode=disable"
	OBJECTS_TBL = "objects"
	TASKS_TBL   = "tasks"

	MAX_WORKERS   = 16
	DEFAULT_DELAY = 1 * time.Second

	WAITING Status = "waiting"
	WORKING Status = "working"
	DONE    Status = "done"
)

var (
	PG = goqu.Dialect("postgres")
)

func seed(db *sql.DB, n int) error {
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < n; i++ {
		task := goqu.Record{
			"id":     xid.New().String(),
			"url":    fmt.Sprintf(DELAY_URL, random.Intn(10)),
			"status": WAITING,
		}

		stmt := PG.Insert(TASKS_TBL).Rows(task)
		q, args, err := stmt.ToSQL()
		if err != nil {
			return err
		}

		if _, err := db.Exec(q, args...); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	db, err := sql.Open("postgres", DSN)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if err := seed(db, 256); err != nil {
		log.Fatal(err)
	}

	objects := make(chan Object)
	done := make(chan bool)
	go storeWorker(db, objects, done)

	wg := &sync.WaitGroup{}
	for i := 0; i < MAX_WORKERS; i++ {
		wg.Add(1)
		go fetchWorker(wg, db, objects)
	}
	wg.Wait()
	close(objects)
	<-done
}
