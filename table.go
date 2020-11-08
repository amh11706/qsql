package qsql

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

var noColumnsErr = errors.New("No columns found. Did you forget the `db:\"column\"` labels in your struct?")
var dbNotReadyErr = errors.New("The database has not been initialized.")

func NewTable(db **sqlx.DB, name string) Table {
	return Table{Name: " " + name + " ", DB: db}
}

type Table struct {
	Name string
	DB   **sqlx.DB
}

// Get an item from the table by its id returning the selected columns.
// If columns are empty, return all that have a "db" tag.
func (t *Table) Get(target interface{}, id int, cols string, args ...interface{}) error {
	return t.GetOptions(target, "WHERE id="+strconv.Itoa(id), cols, args...)
}

// GetOptions gets an item from the table with options returning the selected columns.
// If columns are empty, return all that have a "db" tag.
func (t *Table) GetOptions(target interface{}, options string, cols string, args ...interface{}) error {
	if (*t.DB) == nil {
		return dbNotReadyErr
	}
	if cols == "*" {
		cols = strings.Join(GetColumns(target, false), ",")
	}
	if len(cols) == 0 {
		return noColumnsErr
	}
	return (*t.DB).Get(target, `SELECT `+cols+` FROM`+t.Name+options, args...)
}

// GetAll returns all matching rows with the specified options.
// If columns are empty, return all that have a "db" tag.
func (t *Table) GetAll(target interface{}, options string, cols string, args ...interface{}) error {
	if (*t.DB) == nil {
		return dbNotReadyErr
	}
	if cols == "*" {
		cols = strings.Join(GetColumns(target, false), ",")
	}
	if len(cols) == 0 {
		return noColumnsErr
	}
	return (*t.DB).Select(target, `SELECT `+cols+` FROM`+t.Name+options, args...)
}

// Create inserts a new row into the table.
// If columns are empty, insert all that have a "db" tag except id.
func (t *Table) Create(source interface{}, cols ...string) (sql.Result, error) {
	return t.CreateOptions(source, "", cols...)
}

func (t *Table) CreatePg(source interface{}, cols ...string) (id int, err error) {
	if (*t.DB) == nil {
		return 0, dbNotReadyErr
	}
	if len(cols) == 0 {
		cols = GetColumns(source, true)
		if len(cols) == 0 {
			return 0, noColumnsErr
		}
	} else if len(cols) == 1 {
		cols = strings.Split(cols[0], ",")
	}
	rows, err := (*t.DB).NamedQuery(`INSERT INTO`+t.Name+`(`+strings.Join(cols, ",")+`) VALUES (:`+strings.Join(cols, ",:")+`) RETURNING id`, source)
	if err != nil {
		return
	}
	for rows.Next() {
		err = rows.Scan(&id)
	}
	return
}

// Create inserts a new row into the table.
// If columns are empty, insert all that have a "db" tag except id.
func (t *Table) CreateOptions(source interface{}, options string, cols ...string) (sql.Result, error) {
	if (*t.DB) == nil {
		return nil, dbNotReadyErr
	}
	if len(cols) == 0 {
		cols = GetColumns(source, true)
		if len(cols) == 0 {
			return nil, noColumnsErr
		}
	} else if len(cols) == 1 {
		cols = strings.Split(cols[0], ",")
	}
	return (*t.DB).NamedExec(`INSERT INTO`+t.Name+`SET `+MakeValuesString(cols)+` `+options, source)
}

// Update updates the given struct in the table by its id.
// If columns are empty, update all that have a "db" tag except id.
func (t *Table) Update(source interface{}, cols ...string) (sql.Result, error) {
	return t.UpdateOptions(source, " WHERE id=:id", cols...)
}

// UpdateOptions updates the table with the specified options.
// If columns are empty, update all that have a "db" tag except id.
func (t *Table) UpdateOptions(source interface{}, options string, cols ...string) (sql.Result, error) {
	if (*t.DB) == nil {
		return nil, dbNotReadyErr
	}
	if len(cols) == 0 {
		cols = GetColumns(source, true)
		if len(cols) == 0 {
			return nil, noColumnsErr
		}
	} else if len(cols) == 1 {
		cols = strings.Split(cols[0], ",")
	}
	return (*t.DB).NamedExec(`UPDATE`+t.Name+`SET `+MakeValuesString(cols)+` `+options, source)
}

// Delete a row by its id.
func (t *Table) Delete(id int) (sql.Result, error) {
	return (*t).DeleteOptions("WHERE id=" + strconv.Itoa(id))
}

// DeleteOptions deletes by the provided options instead of a specific id.
func (t *Table) DeleteOptions(options string) (sql.Result, error) {
	if (*t.DB) == nil {
		return nil, dbNotReadyErr
	}
	return (*t.DB).Exec(`DELETE FROM` + t.Name + options)
}

// GetColumns takes a struct and outputs a slice of all db column labels
func GetColumns(from interface{}, skipId bool) []string {
	def := reflect.TypeOf(from)
	kind := def.Kind()
	for kind == reflect.Ptr || kind == reflect.Slice {
		def = def.Elem()
		kind = def.Kind()
	}

	count := def.NumField()
	columns := make([]string, 0, count)
	for i := 0; i < count; i++ {
		f := def.Field(i)
		if t := f.Type.Kind(); t == reflect.Ptr || t == reflect.Struct {
			columns = append(columns, GetColumns(f, skipId)...)
			continue
		}
		column := f.Tag.Get("db")
		if column != "" && !(skipId && column == "id") {
			columns = append(columns, column)
		}
	}
	return columns
}

// MakeValueString is used to prepare a column slice for UPDATE statements
// by mapping to "column=:column" comma separated pairs for sqlx.NamedExec.
func MakeValuesString(cols []string) string {
	prepared := make([]string, len(cols))
	for i, col := range cols {
		prepared[i] = col + "=:" + col
	}
	return strings.Join(prepared, ",")
}

// MakeConflictString is used to prepare a column slice for INSERT statements
// with a ON DUPLICATE KEY clause.
func MakeConflictString(cols []string) string {
	prepared := make([]string, len(cols))
	for i, col := range cols {
		prepared[i] = col + "=VALUES(" + col + ")"
	}
	return strings.Join(prepared, ",")
}
