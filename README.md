# 1. prepare db in docker
./commands.sh run

# 2. run the test to use [go-pg](https://github.com/go-pg/pg/tree/v9) UnionAll
go test -v -run Minimal