package qsql

import (
	"database/sql"
	"strconv"

	"github.com/go-sql-driver/mysql"
)

// LazyString bypasses sql nil scan errors and uses an empty value instead.
type LazyString string

// Scan implements the Scanner interface.
func (ls *LazyString) Scan(value interface{}) (err error) {
	ns := sql.NullString{}
	err = ns.Scan(value)
	*ls = (LazyString(ns.String))
	return
}

// LazyString bypasses sql nil scan errors and uses an empty value instead.
type LazyFloat float64

// Scan implements the Scanner interface.
func (lf *LazyFloat) Scan(value interface{}) (err error) {
	nf := sql.NullFloat64{}
	err = nf.Scan(value)
	*lf = (LazyFloat(nf.Float64))
	return
}

// LazyInt bypasses sql nil scan errors and uses an empty value instead.
type LazyInt int64

// UnmarshalJSON allows this type to unmarshal string encoded ints.
func (li *LazyInt) UnmarshalJSON(data []byte) (err error) {
	dstring := string(data)
	if dstring == "null" {
		*li = 0
		return
	}
	if dstring[:1] == `"` {
		dstring = dstring[1 : len(dstring)-1]
	}
	i, err := strconv.Atoi(dstring)
	*li = (LazyInt(i))
	return
}

// Scan implements the Scanner interface.
func (li *LazyInt) Scan(value interface{}) (err error) {
	ni := sql.NullInt64{}
	err = ni.Scan(value)
	*li = (LazyInt(ni.Int64))
	return
}

// LazyBool bypasses sql nil scan errors and uses an empty value instead.
type LazyBool bool

// Scan implements the Scanner interface.
func (lb *LazyBool) Scan(value interface{}) (err error) {
	nb := sql.NullBool{}
	err = nb.Scan(value)
	*lb = (LazyBool(nb.Bool))
	return
}

// LazyTime bypasses sql nil scan errors and uses an empty value instead.
type LazyTime int64

// Scan implements the Scanner interface.
func (lt *LazyTime) Scan(value interface{}) (err error) {
	nt := mysql.NullTime{}
	err = nt.Scan(value)
	*lt = (LazyTime(nt.Time.Unix()))
	return
}
