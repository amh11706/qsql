package qsql

import (
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

func Connect(driver, host, prefix string, logger func(...interface{})) *sqlx.DB {
	start := time.Now()
	logger(prefix + " connecting to DB...")
	db, err := sqlx.Open(driver, host)
	if err != nil {
		logger(prefix + " failed to connect: " + err.Error())
		os.Exit(1)
		return nil
	}
	db.SetMaxIdleConns(400)
	db.SetMaxOpenConns(400)
	db.SetConnMaxLifetime(20 * time.Minute)

	if err = db.Ping(); err != nil {
		logger(prefix + " failed to connect: " + err.Error())
	} else {
		logger(prefix+" connected to DB in", time.Since(start))
		return db
	}

	os.Exit(1)
	return nil
}
