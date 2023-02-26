package main

import (
	"database/sql"
	"errors"
	"log"

	"github.com/doug-martin/goqu/v9"
)

type Task struct {
	ID     string
	URL    string
	Status Status
}

func startTask(db *sql.DB) (*Task, error) {
	log.Println("starting a task")

	sel := PG.Select(goqu.C("id"), goqu.C("url"), goqu.C("status")).
		From(TASKS_TBL).
		Where(goqu.C("status").Eq(WAITING)).
		Order(goqu.C("id").Asc()).
		Limit(1)
	q, args, err := sel.ToSQL()
	if err != nil {
		return nil, err
	}

	var task Task
	if err := db.QueryRow(q, args...).Scan(&task.ID, &task.URL, &task.Status); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}

		return nil, err
	}

	upd := PG.Update(TASKS_TBL).
		Where(goqu.I("id").Eq(task.ID)).
		Set(goqu.Record{"status": WORKING})
	q, args, err = upd.ToSQL()
	if err != nil {
		return nil, err
	}

	if _, err := db.Exec(q, args...); err != nil {
		return nil, err
	}

	log.Println("started", task.ID)
	return &task, nil
}

func updateTask(db *sql.DB, task Task) error {
	log.Println("updating", task.ID)

	stmt := PG.Update(TASKS_TBL).
		Set(goqu.Record{"status": task.Status}).
		Where(goqu.I("id").Eq(task.ID))
	q, args, err := stmt.ToSQL()
	if err != nil {
		return err
	}

	if _, err := db.Exec(q, args...); err != nil {
		return err
	}

	log.Println("updated", task.ID)
	return nil
}
