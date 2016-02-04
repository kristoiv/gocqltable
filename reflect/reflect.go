// Package reflect provides some punk-rock reflection which is not in the stdlib.
package reflect

import (
	"fmt"
	r "reflect"
	"strings"
	"sync"
)

// StructToMap converts a struct to map. The object's default key string
// is the struct field name but can be specified in the struct field's
// tag value. The "cql" key in the struct field's tag value is the key
// name. Examples:
//
//   // Field appears in the resulting map as key "myName".
//   Field int `cql:"myName"`
//
//   // Field appears in the resulting as key "Field"
//   Field int
//
//   // Field appears in the resulting map as key "myName"
//   Field int "myName"
func StructToMap(val interface{}) (map[string]interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, false
	}
	sinfo := getStructInfo(structVal)
	mapVal := make(map[string]interface{}, len(sinfo.FieldsList))
	for _, field := range sinfo.FieldsList {
		if structVal.Field(field.Num).CanInterface() {
			mapVal[field.Key] = structVal.Field(field.Num).Interface()
		}
	}
	return mapVal, true
}

// MapToStruct converts a map to a struct. It is the inverse of the StructToMap
// function. For details see StructToMap.
func MapToStruct(m map[string]interface{}, struc interface{}) error {
	val := r.Indirect(r.ValueOf(struc))
	sinfo := getStructInfo(val)
	for k, v := range m {
		if info, ok := sinfo.FieldsMap[k]; ok {
			structField := val.Field(info.Num)
			if structField.Type().Name() == r.TypeOf(v).Name() {
				structField.Set(r.ValueOf(v))
			}
		}
	}
	return nil
}

// FieldsAndValues returns a list field names and a corresponing list of values
// for the given struct. For details on how the field names are determined please
// see StructToMap.
func FieldsAndValues(val interface{}) ([]string, []interface{}, bool) {
	// indirect so function works with both structs and pointers to them
	structVal := r.Indirect(r.ValueOf(val))
	kind := structVal.Kind()
	if kind != r.Struct {
		return nil, nil, false
	}
	sinfo := getStructInfo(structVal)
	fields := make([]string, len(sinfo.FieldsList))
	values := make([]interface{}, len(sinfo.FieldsList))
	for i, info := range sinfo.FieldsList {
		field := structVal.Field(info.Num)
		fields[i] = info.Key
		values[i] = field.Interface()
	}
	return fields, values, true
}

type typeMap struct {
	m sync.RWMutex

	types map[r.Type]*StructInfo
}

// Get returns information of a mapped struct if it has been processed by gocqltable.
func (t *typeMap) Get(i interface{}) (*StructInfo, bool) {
	var rt r.Type
	switch v := i.(type) {
	case r.Type:
		rt = v
	default:
		rt = r.Indirect(r.ValueOf(i)).Type()
	}
	t.m.Lock()
	s, found := t.types[rt]
	t.m.Unlock()
	return s, found
}

func (t *typeMap) set(rt r.Type, v *StructInfo) {
	t.m.Lock()
	t.types[rt] = v
	t.m.Unlock()
}

// TypeMap holds all the type's structure information.
var TypeMap = &typeMap{types: make(map[r.Type]*StructInfo)}

// FieldInfo holds information about a struct field.
type FieldInfo struct {
	Key  string
	Num  int
	Type string
}

// StructInfo contains information about a struct's fields.
type StructInfo struct {
	// FieldsMap is used to access fields by their key
	FieldsMap map[string]*FieldInfo
	// FieldsList allows iteration over the fields in their struct order.
	FieldsList []*FieldInfo
}

func getStructInfo(v r.Value) *StructInfo {
	st := r.Indirect(v).Type()
	sinfo, found := TypeMap.Get(st)
	if found {
		return sinfo
	}

	n := st.NumField()
	fieldsMap := make(map[string]*FieldInfo, n)
	fieldsList := make([]*FieldInfo, 0, n)
	for i := 0; i != n; i++ {
		field := st.Field(i)
		info := &FieldInfo{Num: i}
		tag := field.Tag.Get("cql")
		// If there is no cql specific tag and there are no other tags
		// set the cql tag to the whole field tag
		if tag == "" && strings.Index(string(field.Tag), ":") < 0 {
			tag = string(field.Tag)
		}
		if tag != "" {
			info.Key = tag
		} else {
			info.Key = field.Name
		}

		if _, found = fieldsMap[info.Key]; found {
			msg := fmt.Sprintf("Duplicated key '%s' in struct %s", info.Key, st.String())
			panic(msg)
		}

		typeTag := field.Tag.Get("cql_type")
		if typeTag != "" {
			info.Type = typeTag
		}

		fieldsList = append(fieldsList, info)
		fieldsMap[info.Key] = info
	}
	sinfo = &StructInfo{fieldsMap, fieldsList}
	TypeMap.set(st, sinfo)
	return sinfo
}
