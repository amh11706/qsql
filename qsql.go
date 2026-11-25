package qsql

import (
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// A database container started alongside the app (a deploy that recreates both, a host
// reboot) takes a few seconds to listen. Exiting at once would spend the app's few
// restarts on it, so Connect waits for it instead.
const (
	connectWait  = 60 * time.Second
	connectRetry = time.Second
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

	for {
		if err = db.Ping(); err == nil {
			logger(prefix+" connected to DB in", time.Since(start))
			return db
		}
		logger(prefix + " failed to connect: " + err.Error())
		if time.Since(start) >= connectWait {
			break
		}
		time.Sleep(connectRetry)
	}

	os.Exit(1)
	return nil
}
