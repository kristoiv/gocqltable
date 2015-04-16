package gocqltable

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/gocql/gocql"
)

var (
	defaultSession *gocql.Session
)

func SetDefaultSession(s *gocql.Session) {
	defaultSession = s
}

type KeyspaceInterface interface {
	Name() string
	Session() *gocql.Session
}

type Keyspace struct {
	name    string
	session *gocql.Session
}

func NewKeyspace(name string) Keyspace {
	return Keyspace{
		name:    name,
		session: defaultSession,
	}
}

func (ks Keyspace) Create(replication map[string]interface{}, durableWrites bool) error {

	replicationBytes, err := json.Marshal(replication)
	if err != nil {
		return err
	}

	replicationMap := strings.Replace(string(replicationBytes), `"`, `'`, -1)

	durableWritesString := "false"
	if durableWrites {
		durableWritesString = "true"
	}

	return ks.session.Query(fmt.Sprintf(`CREATE KEYSPACE %q WITH REPLICATION = %s AND DURABLE_WRITES = %s`, ks.Name(), replicationMap, durableWritesString)).Exec()

}

func (ks Keyspace) Drop() error {
	return ks.session.Query(fmt.Sprintf(`DROP KEYSPACE %q`, ks.Name())).Exec()
}

func (ks Keyspace) Schema() (string, error) {
	return "", nil
}

func (ks Keyspace) Tables() ([]string, error) {
	var name string
	var resultSet []string
	iterator := ks.session.Query(`SELECT columnfamily_name FROM system.schema_columnfamilies WHERE keyspace_name = ?;`, ks.Name()).Iter()
	for iterator.Scan(&name) {
		resultSet = append(resultSet, name)
	}
	if err := iterator.Close(); err != nil {
		return nil, err
	}
	return resultSet, nil
}

func (ks Keyspace) NewTable(name string, rowKeys, rangeKeys []string, row interface{}) Table {
	return Table{
		name:      name,
		rowKeys:   rowKeys,
		rangeKeys: rangeKeys,
		row:       row,

		keyspace: ks,
		session:  defaultSession,
	}
}

func (ks Keyspace) Name() string {
	return ks.name
}

func (ks Keyspace) Session() *gocql.Session {
	return ks.session
}
