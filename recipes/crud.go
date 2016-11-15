package recipes

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/kristoiv/gocqltable"

	r "github.com/kristoiv/gocqltable/reflect"
)

type RangeInterface interface {
	LessThan(rangeKey string, value interface{}) RangeInterface
	LessThanOrEqual(rangeKey string, value interface{}) RangeInterface
	MoreThan(rangeKey string, value interface{}) RangeInterface
	MoreThanOrEqual(rangeKey string, value interface{}) RangeInterface
	EqualTo(rangeKey string, value interface{}) RangeInterface
	OrderBy(fieldAndDirection string) RangeInterface
	Limit(l int) RangeInterface
	Select(s []string) RangeInterface
	WhereIn(m map[string][]interface{}) RangeInterface
	Fetch() (interface{}, error)
}

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

	// TODO: Test and remove
	// where := []string{}
	// for _, key := range append(rowKeys, rangeKeys...) {
	// 	where = append(where, key+" = ?")
	// }

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
		where = append(where, strings.ToLower(fmt.Sprintf("%q", key))+" = ?")
	}

	row, err := t.Query(fmt.Sprintf(`SELECT * FROM %q.%q WHERE %s LIMIT 1`, t.Keyspace().Name(), t.Name(), strings.Join(where, " AND ")), ids...).FetchRow()
	if err != nil {
		return nil, err
	}

	return row, nil

}

func (t CRUD) List(ids ...interface{}) (interface{}, error) {
	return t.Range(ids...).Fetch()
}

func (t CRUD) Update(row interface{}) error {

	rowKeys := t.RowKeys()
	rangeKeys := t.RangeKeys()

	where := []string{}
	for _, key := range append(rowKeys, rangeKeys...) {
		where = append(where, strings.ToLower(fmt.Sprintf("%q", key))+" = ?")
	}

	m, ok := r.StructToMap(row)
	if !ok {
		panic("Unable to get map from struct during update")
	}

	ids := []interface{}{}
	set := []string{}
	vals := []interface{}{}

	for _, rowKey := range append(rowKeys, rangeKeys...) {
		for key, value := range m {
			if strings.ToLower(key) == strings.ToLower(rowKey) {
				ids = append(ids, value)
				break
			}
		}
	}

	for key, value := range m {
		isAKey := false
		for _, rowKey := range append(rowKeys, rangeKeys...) {
			if strings.ToLower(key) == strings.ToLower(rowKey) {
				isAKey = true
				break
			}
		}
		if !isAKey {
			set = append(set, strings.ToLower(fmt.Sprintf("%q", key))+" = ?")
			vals = append(vals, value)
		}
	}

	if len(ids) < len(rowKeys)+len(rangeKeys) {
		return errors.New(fmt.Sprintf("To few key-values to update row (%d of the required minimum of %d)", len(ids), len(rowKeys)+len(rangeKeys)))
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
		where = append(where, strings.ToLower(fmt.Sprintf("%q", key))+" = ?")
	}

	m, ok := r.StructToMap(row)
	if !ok {
		panic("Unable to get map from struct during update")
	}

	ids := []interface{}{}
	for _, rowKey := range append(rowKeys, rangeKeys...) {
		for key, value := range m {
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

func (t CRUD) Range(ids ...interface{}) RangeInterface {
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
		rangeObj = rangeObj.EqualTo(key, ids[idx]).(Range)
		numAppended++
	}

	return rangeObj
}

type Range struct {
	table gocqltable.TableInterface

	selectCols []string
	where      []string
	whereVals  []interface{}
	order      string
	limit      *int
	filtering  bool
}

func (r Range) LessThan(rangeKey string, value interface{}) RangeInterface {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" < ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) LessThanOrEqual(rangeKey string, value interface{}) RangeInterface {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" <= ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) MoreThan(rangeKey string, value interface{}) RangeInterface {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" > ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) MoreThanOrEqual(rangeKey string, value interface{}) RangeInterface {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" >= ?")
	r.whereVals = append(r.whereVals, value)
	r.filtering = true
	return r
}

func (r Range) EqualTo(rangeKey string, value interface{}) RangeInterface {
	r.where = append(r.where, fmt.Sprintf("%q", strings.ToLower(rangeKey))+" = ?")
	r.whereVals = append(r.whereVals, value)
	return r
}

func (r Range) OrderBy(fieldAndDirection string) RangeInterface {
	r.order = fieldAndDirection
	return r
}

func (r Range) Limit(l int) RangeInterface {
	r.limit = &l
	return r
}

func (r Range) Select(s []string) RangeInterface {
	r.selectCols = s
	return r
}

func (r Range) WhereIn(m map[string][]interface{}) RangeInterface {
	for rangeKey, values := range m {
		numValues := len(values)
		if numValues == 0 {
			continue
		}
		where := fmt.Sprintf("%q IN (", strings.ToLower(rangeKey))
		for i, value := range values {
			r.whereVals = append(r.whereVals, value)
			if i < (numValues - 1) { // append "?, " for all but the last element
				where = where + "?, "
			} else {
				where = where + "?)"
			}
		}
		r.where = append(r.where, where)
	}
	return r
}

func (r Range) Fetch() (interface{}, error) {
	where := r.where
	whereVals := r.whereVals
	order := r.order
	limit := r.limit
	filtering := r.filtering
	selectCols := r.selectCols

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

	selectString := "*"
	if len(selectCols) > 0 {
		selectString = strings.Join(selectCols, ", ")
	}
	query := fmt.Sprintf(`SELECT %s FROM %q.%q %s %s %s %s`, selectString, r.table.Keyspace().Name(), r.table.Name(), whereString, orderString, limitString, filteringString)
	iter := r.table.Query(query, whereVals...).Fetch()

	result := reflect.Zero(reflect.SliceOf(reflect.PtrTo(reflect.TypeOf(r.table.Row())))) // Create a zero-value slice of pointers to our model type
	for row := range iter.Range() {
		result = reflect.Append(result, reflect.ValueOf(row)) // Append the rows to our slice
	}

	if err := iter.Close(); err != nil {
		return nil, err
	}

	return result.Interface(), nil

}
