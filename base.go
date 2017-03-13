package sorm

import (
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strconv"
	"strings"

	"log"

	"database/sql"
)

type AbstractEntity struct {
	SavedValues map[string]interface{}
}

type SqlQuerer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
}
type SqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type SqlQuererExecutor interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func IsValid(a interface{}) bool {
	mustBePtr(a)

	pv := getPrimaryValue(a)
	switch pv.Kind() {
	case reflect.Int:
	case reflect.Int8:
	case reflect.Int16:
	case reflect.Int32:
	case reflect.Int64:
		return getPrimaryValue(a).Int() != 0
	case reflect.Uint:
	case reflect.Uint8:
	case reflect.Uint16:
	case reflect.Uint32:
	case reflect.Uint64:
		return getPrimaryValue(a).Uint() != 0
	}

	return false
}

func setPrimary(a interface{}, newPrimary int64) error {
	pv := getPrimaryValue(a)

	switch pv.Kind() {
	case reflect.Int:
	case reflect.Int8:
	case reflect.Int16:
	case reflect.Int32:
	case reflect.Int64:
		pv.SetInt(newPrimary)
		return nil
	case reflect.Uint:
	case reflect.Uint8:
	case reflect.Uint16:
	case reflect.Uint32:
	case reflect.Uint64:
		pv.SetUint(uint64(newPrimary))
		return nil
	}

	return errors.New("Invalid primary")
}

func LoadEntity(sqe SqlQuererExecutor, a interface{}, column, value interface{}) bool {
	mustBePtr(a)

	rows, err := sqe.Query(fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` = ?", getTableName(a), column), value)
	if err != nil {
		log.Println(err)
		return false
	}

	defer rows.Close()

	columns, err := rows.Columns()
	if err != nil {
		log.Println(err)
	}

	pointers := make([]interface{}, len(columns))
	container := make([]interface{}, len(columns))

	for i, _ := range pointers {
		pointers[i] = &container[i]
	}

	row := rows.Next()
	if !row {
		return false
	}

	rows.Scan(pointers...)

	v := reflect.ValueOf(a).Elem()
	for i, containerValue := range container {
		field := getFieldForDbColumn(v, columns[i])
		setInterfaceToFieldValue(sqe, field, containerValue)
	}

	if !IsValid(a) {
		return false
	}

	SetSavedValues(a)
	return true
}

func setInterfaceToFieldValue(sqe SqlQuererExecutor, field reflect.Value, value interface{}) {
	switch field.Kind() {
	case reflect.Int64:
		field.Set(reflect.ValueOf(value))
		return
	case reflect.String:
		if strVal, ok := value.([]byte); ok {
			field.SetString(string(strVal))
		}
		return
	case reflect.Ptr:
		switch reflect.ValueOf(value).Kind() {
		case reflect.Int64:
			ptrVal := reflect.New(field.Type().Elem())
			LoadEntity(sqe, ptrVal.Interface(), getPrimaryKey(ptrVal.Elem().Interface()), value)
			field.Set(ptrVal)
		case reflect.Invalid:
			return
		case reflect.Struct:
			ptrVal := reflect.New(reflect.TypeOf(value))
			ptrVal.Elem().Set(reflect.ValueOf(value))
			field.Set(ptrVal)
		}
	default:
		field.Set(reflect.ValueOf(value))
	}
}

func SetSavedValues(a interface{}) {
	mustBePtr(a)

	v := reflect.ValueOf(a).Elem()

	savedValuesField := v.FieldByName("SavedValues")
	newMap := make(map[string]interface{})
	savedValuesField.Set(reflect.ValueOf(newMap))

	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		if !isDatabaseField(typeField) {
			continue
		}

		savedValuesField.SetMapIndex(reflect.ValueOf(typeField.Name), v.Field(i))
	}
}

func Save(se SqlExecutor, a interface{}) {
	mustBePtr(a)

	v := reflect.ValueOf(a).Elem()

	var query string
	var data []interface{}
	if !IsValid(a) || v.FieldByName("SavedValues").IsNil() {
		query, data = getInsertQuery(se, a)
	} else {
		query, data = getUpdateQuery(se, a)
	}

	if len(data) == 0 {
		return
	}

	res, err := se.Exec(query, data...)
	if err != nil {
		log.Println(query, data, err)
		return
	}

	if !IsValid(a) {
		newPrimary, err := res.LastInsertId()
		if err == nil && newPrimary != 0 {
			setPrimary(a, newPrimary)
		}
	}

	SetSavedValues(a)
}

func getInsertQuery(se SqlExecutor, a interface{}) (string, []interface{}) {
	mustBePtr(a)

	v := reflect.ValueOf(a).Elem()
	var columns []string
	var queryValues []string
	var values []interface{}

	primaryKey := getPrimaryKey(a)
	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		if len(typeField.PkgPath) != 0 {
			continue
		}

		dbTag := typeField.Tag.Get("db")
		if len(dbTag) == 0 {
			continue
		}

		if dbTag == primaryKey {
			continue
		}

		columns = append(columns, dbTag)
		queryValues = append(queryValues, "?")
		value := v.Field(i).Interface()
		if reflect.Indirect(v.Field(i)).Kind() == reflect.Struct {
			if pk := getPrimaryKey(value); pk != "" {
				Save(se, value)
				value = getPrimaryValue(value).Interface()
			}
		}
		values = append(values, value)
	}

	s := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES (%s)", getTableName(a), strings.Join(columns, ","), strings.Join(queryValues, ","))
	return s, values
}

func getUpdateQuery(se SqlExecutor, a interface{}) (string, []interface{}) {
	mustBePtr(a)

	v := reflect.ValueOf(a).Elem()

	var queryValues []string
	var values []interface{}

	fieldNames := getChangedFieldNames(v)

	for _, fieldName := range fieldNames {
		typeField, _ := v.Type().FieldByName(fieldName)
		queryValues = append(queryValues, fmt.Sprintf("`%s` = ?", typeField.Tag.Get("db")))
		field := v.FieldByName(fieldName)
		value := field.Interface()
		if reflect.Indirect(field).Kind() == reflect.Struct {
			if pk := getPrimaryKey(value); pk != "" {
				Save(se, value)
				value = getPrimaryValue(value).Interface()

			}
		}
		values = append(values, value)
	}

	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = %d", getTableName(a), strings.Join(queryValues, ","), getPrimaryKey(a), getPrimaryValue(a).Int())
	return query, values
}

func getChangedFieldNames(v reflect.Value) []string {
	var fieldNames []string
	savedValuesField := v.FieldByName("SavedValues").Interface()
	savedValues, ok := savedValuesField.(map[string]interface{})
	if !ok {
		panic("baseEntity: SavedValues is not valid map")
	}

	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		if len(typeField.PkgPath) != 0 {
			continue
		}

		if !isDatabaseField(typeField) {
			continue
		}

		if savedValues[v.Type().Field(i).Name] != v.Field(i).Interface() {
			fieldNames = append(fieldNames, v.Type().Field(i).Name)
		}
	}

	return fieldNames
}

func getPrimaryValue(a interface{}) reflect.Value {
	v := reflect.Indirect(reflect.ValueOf(a))
	primaryType := getPrimaryType(v)
	if isValidField(primaryType) {
		return v.FieldByIndex(primaryType.Index)
	} else {
		return reflect.Value{}
	}
}

func getPrimaryKey(a interface{}) string {
	v := reflect.Indirect(reflect.ValueOf(a))
	primaryType := getPrimaryType(v)
	if isValidField(primaryType) {
		return primaryType.Tag.Get("db")
	} else {
		return ""
	}
}

func getPrimaryType(v reflect.Value) reflect.StructField {
	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		if len(typeField.PkgPath) != 0 {
			continue
		}

		primaryTag := typeField.Tag.Get("primary")
		if len(primaryTag) == 0 {
			continue
		}

		primary, err := strconv.ParseBool(primaryTag)
		if err != nil || primary == false {
			continue
		}

		return typeField
	}

	return reflect.StructField{}
}

func getFieldForDbColumn(v reflect.Value, column string) reflect.Value {
	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		if len(typeField.PkgPath) != 0 {
			continue
		}

		primaryTag := typeField.Tag.Get("db")
		if len(primaryTag) != 0 && primaryTag == column {
			return v.Field(i)
		}
	}

	return reflect.Value{}
}

func isDatabaseField(field reflect.StructField) bool {
	return len(field.Tag.Get("db")) > 0
}

func mustBePtr(a interface{}) {
	if !isPtr(a) {
		panic("baseEntity: " + methodName() + " call using non-Ptr parameter")
	}
}

func isPtr(a interface{}) bool {
	return reflect.TypeOf(a).Kind() == reflect.Ptr
}

func getTableName(a interface{}) string {
	name := reflect.Indirect(reflect.ValueOf(a)).Type().Name()
	name = strings.Replace(name, "Entity", "", -1)
	return strings.ToLower(name)
}

func methodName() string {
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "unknown method"
	}
	return f.Name()
}

func getDbFieldsCount(a interface{}) int {
	count := 0
	v := reflect.Indirect(reflect.ValueOf(a))
	for i := 0; i < v.NumField(); i++ {
		typeField := v.Type().Field(i)
		if len(typeField.PkgPath) != 0 {
			continue
		}

		if len(typeField.Tag.Get("db")) != 0 {
			count++
		}
	}

	return count
}

func isValidField(field reflect.StructField) bool {
	return len(field.Index) != 0
}
