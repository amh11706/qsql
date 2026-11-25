package qsql

import "database/sql"

// NullIfZero is the write side of the Lazy types: it stores the zero value of v as NULL,
// so an unset id or an empty name goes into a nullable column as NULL, not 0 or an
// empty string.
//
// @param v the value to store
// @return v as a sql.Null that is valid unless v is the zero value of its type
func NullIfZero[T comparable](v T) sql.Null[T] {
	var zero T
	return sql.Null[T]{V: v, Valid: v != zero}
}
