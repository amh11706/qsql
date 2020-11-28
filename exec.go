package qsql

import (
	"context"
	"database/sql"
)

type DbExecer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

func RowExec(ctx context.Context, tx DbExecer, query string, args ...interface{}) (int64, error) {
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func IdExec(ctx context.Context, tx DbExecer, query string, args ...interface{}) (int64, error) {
	res, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}
