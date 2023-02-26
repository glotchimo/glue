package main

import (
	"database/sql"
	"log"
	"sync"
)

func fetchWorker(wg *sync.WaitGroup, db *sql.DB, objects chan Object) {
	defer wg.Done()
	for {
		task, err := startTask(db)
		if err != nil {
			log.Fatal(err)
		} else if task == nil {
			log.Println("no more tasks")
			return
		}

		obj, err := downloadObject(task.URL)
		if err != nil {
			log.Fatal(err)
		}
		objects <- *obj

		task.Status = DONE
		if err := updateTask(db, *task); err != nil {
			log.Fatal(err)
		}
	}
}

func storeWorker(db *sql.DB, objects chan Object, done chan bool) {
	for obj := range objects {
		if err := saveObject(db, &obj); err != nil {
			log.Fatal(err)
		}
	}
	done <- true
}
