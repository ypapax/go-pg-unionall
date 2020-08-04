package main

import (
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/go-pg/pg/v9"
	"github.com/go-pg/pg/v9/orm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/tjarratt/babble"
)

type Company struct {
	tableName struct{} `pg:"companies"`
	ID        int64
	Name      string
	Customers []*Customer `pg:",many2many:companies_customers"`
}

type Customer struct {
	tableName struct{} `pg:"customers"`
	ID        int64
	Name      string
	Companies []*Company `pg:",many2many:companies_customers"`
}

type CompanyCustomer struct {
	tableName  struct{} `pg:"companies_customers"`
	CompanyID  int64    `pg:"company_id"`
	CustomerID int64    `pg:"customer_id"`
}

const (
	wordsCount = 5
)

var babbler = babble.NewBabbler() // random phrases generator
var connectionString string
var db *pg.DB

func TestMain(m *testing.M) {
	flag.StringVar(&connectionString, "postgres", "postgres://postgres:postgres@localhost:5439/customers?sslmode=disable", "connection string for postgres")
	flag.Parse()
	babbler.Separator = " "
	babbler.Count = wordsCount
	var err error
	db, err = connectToPostgresTimeout(connectionString, 10*time.Second, time.Second)
	if err != nil {
		logrus.Fatalf("%+v", err)
	}
	/*if err := createSchema(db); err != nil {
		logrus.Fatalf("%+v", err)
	}*/
	os.Exit(m.Run())
}

func TestUnionAll(t *testing.T) {
	as := assert.New(t)
	name0 := "customer 1" + babbler.Babble()
	name1 := "customer 2" + babbler.Babble()
	customers := []*Customer{
		{Name: name0},
		{Name: name1},
	}
	for _, cust := range customers {
		if !as.NoError(db.Insert(cust)) {
			return
		}
	}

	com := &Company{
		Name:      babbler.Babble(),
		Customers: customers,
	}
	if !as.NoError(db.Insert(com)) {
		return
	}
	for _, cus := range customers {
		companyCustomer := &CompanyCustomer{CompanyID: com.ID, CustomerID: cus.ID}
		if err := db.Insert(companyCustomer); !as.NoError(err) {
			return
		}
	}
	var model []Customer
	q0 := db.Model(&model).Where("name = ?", name0)
	q0Initial := q0.Clone()
	var q0InitalResult []Customer
	if err := q0Initial.Select(&q0InitalResult); !as.NoError(err) {
		return
	}
	if !as.Len(q0InitalResult, 1, "by name '%s'", name0) {
		return
	}
	q1 := db.Model(&model).Where("name = ?", name1)
	var result []Customer
	if err := q0.UnionAll(q1).Select(&result); !as.NoError(err) {
		return
	}
	if !as.Len(result, 2) {
		return
	}
	if !as.Equal(name0, result[0].Name) {
		return
	}
	if !as.Equal(name1, result[1].Name) {
		return
	}
	t.Logf("result: %+v", result)
}

func TestUnionAllCycle(t *testing.T) {
	as := assert.New(t)
	count := 10
	var customers []*Customer
	for i := 0; i<count; i++ {
		name := fmt.Sprintf("customer %+v", i) + babbler.Babble()
		c := &Customer{Name: name}
		customers = append(customers, c)
		if !as.NoError(db.Insert(c)) {
			return
		}
	}
	com := &Company{
		Name:      babbler.Babble(),
		Customers: customers,
	}
	if !as.NoError(db.Insert(com)) {
		return
	}
	for _, cus := range customers {
		companyCustomer := &CompanyCustomer{CompanyID: com.ID, CustomerID: cus.ID}
		if err := db.Insert(companyCustomer); !as.NoError(err) {
			return
		}
	}
	var model []Customer
	q := db.Model(&model)
	var qUnion *orm.Query
	for i := 0; i<count; i++ {
		qi := q.Clone().Where("name = ?", customers[i].Name)
		if qUnion == nil {
			qUnion = qi
		} else {
			qUnion.UnionAll(qi)
		}
	}


	var result []Customer
	if err := qUnion.Order("name").Select(&result); !as.NoError(err) {
		return
	}
	if !as.Len(result, count) {
		return
	}
	for i := 0; i<count; i++ {
		if !as.Equal(customers[i].Name, result[i].Name) {
			return
		}
	}
	t.Logf("result: %+v", result)
}

func connectToPostgresTimeout(connectionString string, timeout, retry time.Duration) (*pg.DB, error) {
	var (
		connectionError error
		db              *pg.DB
	)
	connected := make(chan bool)
	go func() {
		for {
			db, connectionError = connectToPostgres(connectionString)
			if connectionError != nil {
				time.Sleep(retry)
				continue
			}
			connected <- true
			break
		}
	}()
	select {
	case <-time.After(timeout):
		err := errors.Wrapf(connectionError, "timeout %s connecting to db", timeout)
		return nil, err
	case <-connected:
	}
	return db, nil
}

func connectToPostgres(connectionString string) (*pg.DB, error) {
	opt, err := pg.ParseURL(connectionString)
	if err != nil {
		return nil, errors.Wrap(err, "connecting to postgres with connection string: "+connectionString)
	}

	db := pg.Connect(opt)
	_, err = db.Exec("SELECT 1")
	if err != nil {
		err = errors.WithStack(err)
		return nil, err
	}

	return db, nil
}

func createSchema(db *pg.DB) error {
	for _, model := range []interface{}{(*Company)(nil), (*Customer)(nil)} {
		err := db.CreateTable(model, &orm.CreateTableOptions{
			IfNotExists: true,
			//Temp: true,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
