package recipes

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/elvtechnology/gocqltable"

	r "github.com/elvtechnology/gocqltable/reflect"
)

type CRUD struct {
	gocqltable.TableInterface
}

func (t CRUD) Insert(row interface{}) error {
	return t.insert(row, nil)
}

func (t CRUD) InsertWithTTL(row interface{}, ttl *time.Time) error {
	return t.insert(row, ttl)
}

func (t CRUD) insert(row interface{}, ttl *time.Time) error {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	where := []string{}
	for _, key := range append(rowKeys, rangeKeys...) {
		where = append(where, key+" = ?")
	}

	m, ok := r.StructToMap(row)
	if !ok {
		panic("Unable to get map from struct during update")
	}

	fields := []string{}
	placeholders := []string{}
	vals := []interface{}{}
	for key, value := range m {
		// Check for empty row- or range keys
		for _, rowKey := range append(rowKeys, rangeKeys...) {
			if strings.ToLower(key) == strings.ToLower(rowKey) {
				if value == nil {
					return errors.New(fmt.Sprintf("Inserting row failed due to missing key value (for key %q)", rowKey))
				}
				break
			}
		}
		// Append to insertion slices
		fields = append(fields, strings.ToLower(fmt.Sprintf("%q", key)))
		placeholders = append(placeholders, "?")
		vals = append(vals, value)
	}

	options := ""
	if ttl != nil {
		options = "USING TTL ?"
		vals = append(vals, int(ttl.Sub(time.Now().UTC()).Seconds()+.5))
	}

	err := t.Query(fmt.Sprintf(`INSERT INTO %q.%q (%s) VALUES (%s) %s`, t.Keyspace().Name(), t.Name(), strings.Join(fields, ", "), strings.Join(placeholders, ", "), options), vals...).Exec()
	if err != nil {
		for _, v := range vals {
			log.Printf("%T %v", v, v)
		}
		return err
	}

	return nil

}

func (t CRUD) Get(ids ...interface{}) (interface{}, error) {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	if len(ids) < len(rowKeys)+len(rangeKeys) {
		return nil, errors.New(fmt.Sprintf("To few key-values to query for row (%d of the required %d)", len(ids), len(rowKeys)+len(rangeKeys)))
	}

	where := []string{}
	for _, key := range append(rowKeys, rangeKeys...) {
		where = append(where, key+" = ?")
	}

	row, err := t.Query(fmt.Sprintf(`SELECT * FROM %q.%q WHERE %s LIMIT 1`, t.Keyspace().Name(), t.Name(), strings.Join(where, " AND ")), ids...).FetchRow()
	if err != nil {
		return nil, err
	}

	return row, nil

}

func (t CRUD) List(ids ...interface{}) ([]interface{}, error) {
	return t.Range(ids...).Fetch()
}

func (t CRUD) Update(row interface{}) error {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	where := []string{}
	for _, key := range append(rowKeys, rangeKeys...) {
		where = append(where, key+" = ?")
	}

	m, ok := r.StructToMap(row)
	if !ok {
		panic("Unable to get map from struct during update")
	}

	ids := []interface{}{}
	set := []string{}
	vals := []interface{}{}
	for key, value := range m {
		isAKey := false
		for _, rowKey := range append(rowKeys, rangeKeys...) {
			if strings.ToLower(key) == strings.ToLower(rowKey) {
				ids = append(ids, value)
				isAKey = true
				break
			}
		}

		if !isAKey {
			set = append(set, key+" = ?")
			vals = append(vals, value)
		}
	}

	if len(ids) < len(rowKeys) {
		return errors.New(fmt.Sprintf("To few key-values to query for row (%d of the required minimum of %d)", len(ids), len(rowKeys)))
	}

	err := t.Query(fmt.Sprintf(`UPDATE %q.%q SET %s WHERE %s`, t.Keyspace().Name(), t.Name(), strings.Join(set, ", "), strings.Join(where, " AND ")), append(vals, ids...)...).Exec()
	if err != nil {
		return err
	}
	return nil

}

func (t CRUD) Delete(row interface{}) error {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	where := []string{}
	for _, key := range append(rowKeys, rangeKeys...) {
		where = append(where, key+" = ?")
	}

	m, ok := r.StructToMap(row)
	if !ok {
		panic("Unable to get map from struct during update")
	}

	ids := []interface{}{}
	for key, value := range m {
		for _, rowKey := range append(rowKeys, rangeKeys...) {
			if strings.ToLower(key) == strings.ToLower(rowKey) {
				ids = append(ids, value)
				break
			}
		}
	}

	if len(ids) < len(rowKeys)+len(rangeKeys) {
		return errors.New(fmt.Sprintf("To few key-values to delete row (%d of the required %d)", len(ids), len(rowKeys)+len(rangeKeys)))
	}

	err := t.Query(fmt.Sprintf(`DELETE FROM %q.%q WHERE %s`, t.Keyspace().Name(), t.Name(), strings.Join(where, " AND ")), ids...).Exec()
	if err != nil {
		return err
	}
	return nil

}

func (t CRUD) Range(ids ...interface{}) Range {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	rangeObj := Range{
		table: t,
	}

	numAppended := 0
	for idx, key := range append(rowKeys, rangeKeys...) {
		if len(ids) == numAppended {
			break
		}
		rangeObj = rangeObj.EqualTo(key, ids[idx])
		numAppended += 1
	}

	return rangeObj
}

type Range struct {
	table gocqltable.TableInterface

	where     []string
	whereVals []interface{}
	order     string
	limit     *int
	filtering bool
}

func (r Range) LessThan(rangeKey string, value interface{}) Range {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" < ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) LessThanOrEqual(rangeKey string, value interface{}) Range {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" <= ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) MoreThan(rangeKey string, value interface{}) Range {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" > ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) MoreThanOrEqual(rangeKey string, value interface{}) Range {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" >= ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) EqualTo(rangeKey string, value interface{}) Range {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" = ?")
	r.whereVals = append(r.whereVals, value)
	return r
}

func (r Range) OrderBy(fieldAndDirection string) Range {
	r.order = fieldAndDirection
	return r
}

func (r Range) Limit(l int) Range {
	r.limit = &l
	return r
}

func (r Range) Fetch() ([]interface{}, error) {

	where := r.where
	whereVals := r.whereVals
	order := r.order
	limit := r.limit
	filtering := r.filtering

	whereString := ""
	if len(where) > 0 {
		whereString = "WHERE " + strings.Join(where, " AND ")
	}

	orderString := ""
	if order != "" {
		orderString = fmt.Sprintf("ORDER BY %v", order)
	}

	limitString := ""
	if limit != nil {
		limitString = "LIMIT " + strconv.Itoa(*limit)
	}

	filteringString := ""
	if filtering {
		filteringString = "ALLOW FILTERING"
	}

	iter := r.table.Query(fmt.Sprintf(`SELECT * FROM %q.%q %s %s %s %s`, r.table.Keyspace().Name(), r.table.Name(), whereString, orderString, limitString, filteringString), whereVals...).Fetch()
	result := []interface{}{}
	for row := range iter.Range() {
		result = append(result, row)
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return result, nil

}
