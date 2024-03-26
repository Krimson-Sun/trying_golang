package main

import "errors"

var store = make(map[string]string)

var ErrNoSuchKey = errors.New("no such key")

func Get(key string) (string, error) {
	if val, ok := store[key]; ok {
		return val, nil
	}
	return "", ErrNoSuchKey
}

func Put(key, value string) error {
	store[key] = value

	return nil
}

func Delete(key string) error {
	delete(store, key)

	return nil
}
