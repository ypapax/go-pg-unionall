package main

import (
	"context"
	"fmt"
	"github.com/go-pg/pg/v9"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type dbLogger struct {
	LogExecutionTime         bool
	ConnectionString         string
}

//BeforeQuery(context.Context, *QueryEvent) (context.Context, error)
//AfterQuery(context.Context, *QueryEvent) error

func (d *dbLogger) BeforeQuery(ctx context.Context, q *pg.QueryEvent) (context.Context, error) {
	qStr, err := q.FormattedQuery() // Needed when PG Query is failing
	if err != nil {
		logrus.Errorf("%+v", err)
	}
	logrus.Tracef("query to execute: %s", qStr)
	return ctx, nil
}

func (d *dbLogger) AfterQuery(ctx context.Context, q *pg.QueryEvent) error {
	var fields = make(logrus.Fields)
	fields["conn-str"] = d.ConnectionString
	lo := logrus.WithFields(fields)
	qStr, err := q.FormattedQuery()
	if err != nil {
		lo.Error(err)
		return errors.WithStack(err)
	}
	var aboutErr string
	if q.Err != nil {
		aboutErr = fmt.Sprintf(", query error: %+v", q.Err)
	}
	lo.Tracef("%s executed query: %s %+v", time.Since(q.StartTime), qStr, aboutErr)
	return nil
}

var addedDbLoggerMtx = sync.Mutex{}
var addedDbLogger = make(map[string]struct{})

func AddDbLogger(db *pg.DB, logExecutionTime bool, loggerName string) error {
	addedDbLoggerMtx.Lock()
	defer addedDbLoggerMtx.Unlock()
	lo := logrus.WithField("logger-name", loggerName).WithField("logExecutionTime", logExecutionTime)
	if _, added := addedDbLogger[loggerName]; added {
		if added {
			lo.Tracef("this db is already added for tracking")
			return nil
		}
	}

	var l = &dbLogger{}
	l.LogExecutionTime = logExecutionTime
	l.ConnectionString = loggerName
	db.AddQueryHook(l)
	addedDbLogger[loggerName] = struct{}{}
	lo.Infof("added db logger")
	return nil
}

func (d *dbLogger) queryKey(q *pg.QueryEvent) (string, error) {
	return fmt.Sprintf("%p%+v", q, d.ConnectionString), nil
}
