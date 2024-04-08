package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"sync"
)

type PgTransactionLogger struct {
	events chan<- Event
	errors <-chan error
	db     *sql.DB
	wg     *sync.WaitGroup
}

func (l *PgTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{
		EventType: EventPut,
		key:       key,
		value:     value,
	}
}

func (l *PgTransactionLogger) WriteDelete(key string) {
	l.events <- Event{
		EventType: EventDelete,
		key:       key,
	}
}

func (l *PgTransactionLogger) Err() <-chan error {
	return l.errors
}

type PgDbparams struct {
	DbName   string
	host     string
	user     string
	password string
	port     int
}

func NewPgTransactionLogger(params PgDbparams) (TransactionLogger, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		params.host, params.port, params.user, params.password, params.DbName))
	if err != nil {
		return nil, fmt.Errorf("db connection failed %w", err)
	} else {
		fmt.Println("db connection success")
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("db ping failed %w", err)
	}
	lgr := &PgTransactionLogger{db: db, wg: &sync.WaitGroup{}}

	if err = lgr.createTable(); err != nil {
		return nil, fmt.Errorf("create table failed %w", err)
	}

	return lgr, nil

}

func (l *PgTransactionLogger) createTable() error {
	var err error
	_, err = l.db.Exec(`CREATE TABLE IF NOT EXISTS transactions (
		sequence 	BIGSERIAL PRIMARY KEY,
		event_type 	int NOT NULL,
		key 		TEXT NOT NULL,
		value 		TEXT
	);`)
	if err != nil {
		return err
	}

	return nil
}

func (l *PgTransactionLogger) Run() {
	events := make(chan Event)
	l.events = events
	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		query := `INSERT INTO transactions (event_type, key, value) VALUES ($1, $2, $3)`

		for event := range events {
			_, err := l.db.Exec(query, event.EventType, event.key, event.value)
			if err != nil {
				errors <- err
			}
			l.wg.Done()
		}
	}()
}

func (l *PgTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	outEvent := make(chan Event)
	outError := make(chan error)

	go func() {
		defer close(outEvent)
		defer close(outError)

		rows, err := l.db.Query(`SELECT sequence, event_type, key, value FROM transactions ORDER BY sequence`)
		if err != nil {
			outError <- err
			return
		}

		defer rows.Close()

		e := Event{}

		for rows.Next() {
			err = rows.Scan(&e.Sequence, &e.EventType, &e.key, &e.value)

			if err != nil {
				outError <- fmt.Errorf("error reading from log file: %w", err)
			}
			outEvent <- e
		}

		err = rows.Err()
		if err != nil {
			outError <- fmt.Errorf("error reading from log file: %w", err)
		}
	}()
	return outEvent, outError
}
