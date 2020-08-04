#!/usr/bin/env bash
set -ex
set -o pipefail

run() {
  docker-compose build
  docker-compose up
}

rerun() {
  set +e
  docker kill postgres
  docker rm postgres
  set -e
  run
}

sql() {
  psql -h localhost -p 5439 -U postgres -d customers -c "$@"
}

all(){
  sql "select * from customers;"
  sql "select * from companies;"
  sql "select * from companies_customers;"
}

"$@"
