package polluter

import (
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	txdb "github.com/DATA-DOG/go-txdb"
)

func init() {
	txdb.Register("mysqltx", "mysql", "root:root123@tcp(localhost:3306)/test")
	txdb.Register("pgsqltx", "postgres", "password=root123 user=postgres dbname=postgres sslmode=disable")
}
