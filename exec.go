package qsql

import "database/sql"

type DbExecer interface {
	Exec(string, ...interface{}) (sql.Result, error)
}

func RowExec(tx DbExecer, query string, args ...interface{}) (int64, error) {
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func IdExec(tx DbExecer, query string, args ...interface{}) (int64, error) {
	res, err := tx.Exec(query, args...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
