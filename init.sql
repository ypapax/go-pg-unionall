CREATE DATABASE customers;
\connect customers;
CREATE TABLE customers
(
    id   bigserial NOT NULL,
    name text      NULL,
    CONSTRAINT customers_pk PRIMARY KEY (id)

);

CREATE TABLE companies
(
    id   bigserial NOT NULL,
    name text      NULL,
    CONSTRAINT companies_pk PRIMARY KEY (id)
);

CREATE TABLE companies_customers
(
    id          bigserial NOT NULL,
    company_id  int8      NOT NULL REFERENCES companies (id),
    customer_id int8      NOT NULL REFERENCES customers (id)
);
