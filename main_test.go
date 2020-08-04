package main

import (
	"flag"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/go-pg/pg"
	"github.com/go-pg/pg/orm"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tjarratt/babble"
)

type Company struct {
	TableName struct{} `sql:"companies"`
	ID        int64
	Name      string
	Customers []*Customer `pg:",many2many:companies_customers"`
}

type Customer struct {
	TableName struct{} `sql:"customers"`
	ID        int64
	Name      string
	Companies []*Company `pg:",many2many:companies_customers"`
}

type CompanyCustomer struct {
	TableName  struct{} `sql:"companies_customers"`
	CompanyID  int64    `sql:"company_id"`
	CustomerID int64    `sql:"customer_id"`
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

func TestTwoCustomers(t *testing.T) {
	as := assert.New(t)
	customers := []*Customer{
		{Name: "customer 1" + babbler.Babble()},
		{Name: "customer 2" + babbler.Babble()},
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
	var compSelect Company
	if !as.NoError(db.Model(&compSelect).Column("Customers").Where("company.name = ?", com.Name).Select()) {
		return
	}
	if !as.NotZero(compSelect.ID) {
		return
	}
	if !as.Len(compSelect.Customers, len(customers)) {
		return
	}

	for i, c1 := range customers {
		as.Equal(c1.Name, compSelect.Customers[i].Name)
	}
}

func TestTwoCompanies(t *testing.T) {
	as := assert.New(t)
	companies := []*Company{
		{Name: "company 1" + babbler.Babble()},
		{Name: "company 2" + babbler.Babble()},
	}
	for _, comp := range companies {
		if !as.NoError(db.Insert(comp)) {
			return
		}
	}

	cust := &Customer{
		Name:      babbler.Babble(),
		Companies: companies,
	}
	if !as.NoError(db.Insert(cust)) {
		return
	}
	for _, com := range companies {
		companyCustomer := &CompanyCustomer{CompanyID: com.ID, CustomerID: cust.ID}
		if err := db.Insert(companyCustomer); !as.NoError(err) {
			return
		}
	}
	var custSelect Customer
	if !as.NoError(db.Model(&custSelect).Column("Companies").Where("customer.name = ?", cust.Name).Select()) {
		return
	}
	t.Logf("custSelect %+v", custSelect)
	if !as.Equal(cust.Name, custSelect.Name) {
		return
	}
	if !as.NotZero(custSelect.ID) {
		return
	}
	if !as.Len(custSelect.Companies, len(companies)) {
		return
	}
	for i, c1 := range companies {
		as.Equal(c1.Name, custSelect.Companies[i].Name)
	}
	for _, comp := range custSelect.Companies {
		t.Logf("company %+v", comp)
	}
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
