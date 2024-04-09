package main

import (
	"log"

	"github.com/chrisarmitage/poc-go-user-auth-dynamodb/ddb"
)

func main() {
	port := 9000
	tableName := "users"

	client, err := ddb.NewLocalClient(port)
	if err != nil {
		log.Println("unable to connect to dynamodb server:", err)
	}

	exists, err := client.TableExists(tableName)
	if err != nil {
		log.Println("unable to check if table exists:", err)
	}

	if !exists {
		log.Println("creating user table")
		err := client.CreateUserTable()
		if err != nil {
			log.Println("unable to create table:", err)
		}
	} else {
		log.Println("skipping creating user table, already exists")
	}
}
