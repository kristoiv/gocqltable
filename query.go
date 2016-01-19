package gocqltable

import (
	"reflect"
	"strings"

	"github.com/gocql/gocql"

	r "github.com/kristoiv/gocqltable/reflect"
)

type Query struct {
	Statement string
	Values    []interface{}

	Table   Table
	Session *gocql.Session
}

func (q Query) FetchRow() (interface{}, error) {
	iter := q.Fetch()
	row := iter.Next()
	if err := iter.Close(); err != nil {
		return nil, err
	}
	if row == nil {
		return nil, gocql.ErrNotFound
	}
	return row, nil
}

func (q Query) Fetch() *Iterator {
	iter := q.Session.Query(q.Statement, q.Values...).Iter()
	return &Iterator{
		iter: iter,
		row:  q.Table.Row(),
	}
}

func (q Query) Exec() error {
	return q.Session.Query(q.Statement, q.Values...).Exec()
}

type Iterator struct {
	iter *gocql.Iter
	row  interface{}

	done chan bool
}

func (i *Iterator) Next() interface{} {
	m := make(map[string]interface{})
	if i.iter.MapScan(m) == false {
		return nil
	}
	t := reflect.TypeOf(i.row)
	v := reflect.New(t)
	r.MapToStruct(m, v.Interface())
	r.MapToStruct(ucfirstKeys(m), v.Interface())
	return v.Interface()
}

func (i *Iterator) Range() <-chan interface{} {
	rangeChan := make(chan interface{})
	done := make(chan bool)
	i.done = done
	go func() {
		for {
			next := i.Next()
			if next == nil { // We clean up when we're done
				close(rangeChan)
				return
			}
			select {
			case <-done: // We clean up when we're done
				close(rangeChan)
				return
			case rangeChan <- next:
			}
		}
	}()
	return rangeChan
}

func (i *Iterator) Close() error {
	if done := i.done; done != nil {
		close(i.done)
		i.done = nil
	}
	return i.iter.Close()
}

func ucfirst(s string) string {
	if len(s) < 2 {
		return strings.ToUpper(s)
	}
	return strings.ToUpper(s[0:1]) + s[1:]
}

func ucfirstKeys(s map[string]interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	for key, value := range s {
		result[ucfirst(key)] = value
	}
	return result
}
