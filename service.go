package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var logger TransactionLogger

func initTransactionLogger() error {
	var err error
	logger, err = NewFileTransactionLogger("transactions.log")

	if err != nil {
		return fmt.Errorf("failed when create event logger: %w", err)
	}

	events, errors := logger.ReadEvents()
	e, ok := Event{}, true

	for ok && err == nil {
		select {
		case err, ok = <-errors:

		case e, ok = <-events:
			switch e.EventType {
			case EventDelete:
				err = Delete(e.key)
			case EventPut:
				err = Put(e.key, e.value)
			}
		}
	}
	log.Printf("events replayed\n")
	logger.Run()

	return err
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	log.Print(vars)

	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	log.Print(value)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	err = Put(key, string(value))

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WritePut(key, string(value))

	w.WriteHeader(http.StatusCreated)

	log.Print("PUT ", key, " ", value)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)

	if errors.Is(err, ErrNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))

	log.Print("GET ", key, " ", value)
}

func keyDeleteValueHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	logger.WriteDelete(key)

	log.Print("DELETE ", key)

}

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func NotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
}

func main() {
	err := initTransactionLogger()
	if err != nil {
		panic(err)
	}
	fmt.Println("Transaction logger initialized")

	r := mux.NewRouter()

	r.Use(loggingMiddleware)

	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods(http.MethodGet)
	r.HandleFunc("/v1/{key}", keyDeleteValueHandler).Methods(http.MethodDelete)

	r.HandleFunc("/v1/{key}", NotAllowedHandler)
	r.HandleFunc("/v1", NotAllowedHandler)

	log.Fatal(http.ListenAndServe(":8080", r))
}
