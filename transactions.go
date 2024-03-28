package main

import (
	"bufio"
	"fmt"
	"os"
	"sync"
)

type EventType byte

const (
	_                     = iota
	EventDelete EventType = iota
	EventPut
)

type Event struct {
	Sequence  uint64
	EventType EventType
	key       string
	value     string
}

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan Event, <-chan error)
	Run()
}

type fileTransactionLogger struct {
	events chan<- Event
	errors <-chan error

	file         *os.File
	lastSequence uint64
	wg           *sync.WaitGroup
}

func (l *fileTransactionLogger) WritePut(key, value string) {
	l.wg.Add(1)
	l.events <- Event{EventType: EventPut, key: key, value: value}
}

func (l *fileTransactionLogger) WriteDelete(key string) {
	l.events <- Event{EventType: EventDelete, key: key}
}

func (l *fileTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	var err error
	var lgr = fileTransactionLogger{wg: &sync.WaitGroup{}}
	lgr.file, err = os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		return nil, fmt.Errorf("error opening file: %w", err)
	}
	return &lgr, nil
}

func (l *fileTransactionLogger) Run() {
	events := make(chan Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	go func() {
		for e := range events {
			l.lastSequence++

			_, err := fmt.Fprintf(l.file, "%d\t%d\t%s\t%s\n", l.lastSequence, e.EventType, e.key, e.value)

			if err != nil {
				errors <- fmt.Errorf("error writing to log file: %w", err)
			}
			l.wg.Done()
		}

	}()

}

func (l *fileTransactionLogger) ReadEvents() (<-chan Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan Event)
	outError := make(chan error)

	go func() {
		var e Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s\n",
				&e.Sequence, &e.EventType, &e.key, &e.value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}

			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction number out of sequence: %d >= %d", l.lastSequence, e.Sequence)
				return
			}
			l.lastSequence = e.Sequence
			outEvent <- e
		}

		if err := scanner.Err(); err != nil {
			fmt.Println(err)
			outError <- fmt.Errorf("error reading from log file: %w", err)
			return
		}

	}()

	return outEvent, outError

}
