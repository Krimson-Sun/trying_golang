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
}

func NewPgTransactionLogger(params PgDbparams) (TransactionLogger, error) {
	db, err := sql.Open("postgres", fmt.Sprintf("host=%s user=%s password=%s dbname=%s sslmode=disable",
		params.host, params.user, params.password, params.DbName))
	if err != nil {
		return nil, fmt.Errorf("db connection failed %w", err)
	}

	err = db.Ping()
	if err != nil {
		return nil, fmt.Errorf("db ping failed %w", err)
	}
	lgr := &PgTransactionLogger{db: db, wg: &sync.WaitGroup{}}

	exist, err := lgr.verifyTableExists()
	if err != nil {
		return nil, fmt.Errorf("table exists failed %w", err)
	}
	if !exist {
		if err = lgr.createTable(); err != nil {
			return nil, fmt.Errorf("create table failed %w", err)
		}
	}

	return lgr, nil

}

func (l *PgTransactionLogger) verifyTableExists() (bool, error) {
	const table = "transactions"

	var result string

	rows, err := l.db.Query(fmt.Sprintf("SELECT to_regclass(public.%s)", table))
	defer rows.Close()
	if err != nil {
		return false, err
	}

	for rows.Next() && result != table {
		rows.Scan(&result)
	}

	return table == result, rows.Err()
}

func (l *PgTransactionLogger) createTable() error {
	var err error
	_, err = l.db.Exec(`CREATE TABLE transactions (
		sequence 	BIGSERIAL PRIMARY KEY,
		even_type 	BYTE NOT NULL,
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
