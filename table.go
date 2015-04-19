package gocqltable

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"

	r "github.com/elvtechnology/gocqltable/reflect"
)

type TableInterface interface {
	Create() error
	Drop() error
	Query(statement string, params ...interface{}) Query
	Name() string
	Keyspace() Keyspace
	RowKeys() []string
	RangeKeys() []string
	Row() interface{}
}

type Table struct {
	name      string
	rowKeys   []string
	rangeKeys []string
	row       interface{}

	keyspace Keyspace
	session  *gocql.Session
}

func (t Table) Create() error {
	return t.create()
}

func (t Table) CreateWithProperties(props ...string) error {
	return t.create(props...)
}

func (t Table) create(props ...string) error {

	if t.session == nil {
		t.session = defaultSession
	}

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	pkString := "PRIMARY KEY ((" + strings.Join(rowKeys, ", ") + ")"
	if len(rangeKeys) > 0 {
		pkString = pkString + ", " + strings.Join(rangeKeys, ", ")
	}
	pkString = pkString + ")"

	fields := []string{}

	m, ok := r.StructToMap(t.Row())
	if !ok {
		panic("Unable to get map from struct during create table")
	}

	for key, value := range m {
		key = strings.ToLower(key)
		typ, err := stringTypeOf(value)
		if err != nil {
			return err
		}
		fields = append(fields, fmt.Sprintf(`%q %v`, key, typ))
	}

	// Add primary key value to fields list
	fields = append(fields, pkString)

	propertiesString := ""
	if len(props) > 0 {
		propertiesString = "WITH " + strings.Join(props, " AND ")
	}

	return t.session.Query(fmt.Sprintf(`CREATE TABLE %q.%q (%s) %s`, t.Keyspace().Name(), t.Name(), strings.Join(fields, ", "), propertiesString)).Exec()

}

func (t Table) Drop() error {
	if t.session == nil {
		t.session = defaultSession
	}
	return t.session.Query(fmt.Sprintf(`DROP TABLE %q.%q`, t.Keyspace().Name(), t.Name())).Exec()
}

func (t Table) Query(statement string, values ...interface{}) Query {
	if t.session == nil {
		t.session = defaultSession
	}
	return Query{
		Statement: statement,
		Values:    values,

		Table:   t,
		Session: t.session,
	}
}

func (t Table) Name() string {
	return t.name
}

func (t Table) Keyspace() Keyspace {
	return t.keyspace
}

func (t Table) RowKeys() []string {
	return t.rowKeys
}

func (t Table) RangeKeys() []string {
	return t.rangeKeys
}

func (t Table) Row() interface{} {
	return t.row
}
