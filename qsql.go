package qsql

import (
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var DB *sqlx.DB

func Connect(driver, host, prefix string, logger func(...interface{})) *sqlx.DB {
	start := time.Now()
	logger(prefix + " connecting to DB...")
	db, err := sqlx.Open(driver, host)
	db.SetMaxIdleConns(400)
	db.SetMaxOpenConns(400)
	db.SetConnMaxLifetime(20 * time.Minute)
	if err != nil {
		logger(prefix + " failed to connect: " + err.Error())
		db.Close()
		return nil
	}

	for {
		if err = db.Ping(); err != nil {
			logger(prefix + " failed to connect: " + err.Error())
		} else {
			logger(prefix+" connected to DB in", time.Since(start))
			DB = db
			return db
		}

		time.Sleep(10 * time.Second)
	}
}
