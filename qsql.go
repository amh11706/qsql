package qsql

import (
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
		time.Sleep(5 * time.Second)
		return Connect(driver, host, prefix, logger)
	}
	db.SetMaxIdleConns(400)
	db.SetMaxOpenConns(400)
	db.SetConnMaxLifetime(20 * time.Minute)

	for {
		if err = db.Ping(); err != nil {
			logger(prefix + " failed to connect: " + err.Error())
		} else {
			logger(prefix+" connected to DB in", time.Since(start))
			return db
		}

		time.Sleep(10 * time.Second)
	}
}
