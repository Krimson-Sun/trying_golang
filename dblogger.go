package main

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"sync"
)

type PgTransactionLogger struct {
	event  chan<- Event
	errors <-chan error
	db     *sql.DB
	wg     *sync.WaitGroup
}

func (l *PgTransactionLogger) WritePut(key, value string) {
	l.event <- Event{
		EventType: EventPut,
		key:       key,
		value:     value,
	}
}

func (l *PgTransactionLogger) WriteDelete(key string) {
	l.event <- Event{
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
		sequence BIGSERIAL PRIMARY KEY,
		even_type BYTE NOT NULL,
		key TEXT NOT NULL,
		value TEXT
	);`)
	if err != nil {
		return err
	}

	return nil
}

func (l *PgTransactionLogger) Run() {
	// TODO
}

func (l *PgTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	return nil, nil // TODO
}
