package main

import (
	"fmt"
	"log"
	"time"

	"github.com/elvtechnology/gocqltable"
	"github.com/gocql/gocql"
)

func main() {

	// Generic initialization of gocql
	c := gocql.NewCluster("127.0.0.1")
	s, err := c.CreateSession()
	if err != nil {
		log.Fatalln("Unable to open up a session with the Cassandra database (err=" + err.Error() + ")")
	}

	// Tell gocqltable to use this session object as the default for new objects
	gocqltable.SetDefaultSession(s)
	fmt.Println("Gocql session setup complete")

	// Now we're ready to create our first keyspace. We start by getting a keyspace object
	keyspace := gocqltable.NewKeyspace("gocqltable_test")

	// Now lets create that in the database using the simple strategy and durable writes (true)
	err = keyspace.Create(map[string]interface{}{
		"class":              "SimpleStrategy",
		"replication_factor": 1,
	}, true)
	if err != nil { // If something went wrong we print the error and quit.
		log.Fatalln(err)
	}
	fmt.Println("Keyspace created")

	// Now that we have a very own keyspace to play with, lets create our first table.

	// First we need a Row-object to base the table on. It will later be passed to the table wrapper
	// to be used for returning row-objects as the answer to fetch requests.
	type User struct {
		Email    string // Our primary key
		Password string
		Active   bool
		Created  time.Time
	}

	// Let's define and instantiate a table object for our user table
	userTable := struct {
		gocqltable.Table
	}{
		keyspace.NewTable(
			"users",           // The table name
			[]string{"email"}, // Row keys
			nil,               // Range keys
			User{},            // We pass an instance of the user struct that will be used as a type template during fetches.
		),
	}

	// Lets create this table in our cassandra database
	err = userTable.Create()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Table created: users")

	// Now that we have a keyspace with a table in it: lets make a few rows! Notice that this is the base example, it uses CQL (not ORM)
	// for database interactions such as INSERT/SELECT/UPDATE/DELETE.
	err = userTable.Query("INSERT INTO gocqltable_test.users (email, password, active, created) VALUES (?, ?, ?, ?)", "1@example.com", "123456", true, time.Now().UTC()).Exec()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("User inserted: 1@example.com")

	err = userTable.Query("INSERT INTO gocqltable_test.users (email, password, active, created) VALUES (?, ?, ?, ?)", "2@example.com", "123456", true, time.Now().UTC()).Exec()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("User inserted: 2@example.com")

	err = userTable.Query("INSERT INTO gocqltable_test.users (email, password, active, created) VALUES (?, ?, ?, ?)", "3@example.com", "123456", true, time.Now().UTC()).Exec()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("User inserted: 3@example.com")

	// With our database filled up with users, lets query it and print out the results.
	iter := userTable.Query("SELECT * FROM gocqltable_test.users").Fetch()
	fmt.Println("")
	fmt.Println("Fetched all from users:")
	for row := range iter.Range() {
		user := row.(*User)        // Our row variable is a pointer to "interface{}", and here we type assert it to a pointer to "User"
		fmt.Println("User:", user) // Let's just print that
	}
	if err := iter.Close(); err != nil {
		log.Fatalln(err)
	}

	// You can also fetch a single row, obviously
	row, err := userTable.Query(`SELECT * FROM gocqltable_test.users WHERE email = ? LIMIT 1`, "2@example.com").FetchRow()
	if err != nil {
		log.Fatalln(err)
	}
	user := row.(*User)
	fmt.Println("")
	fmt.Println("Fetched single row by email: ", user)

	// Lets clean up after ourselves by dropping the keyspace.
	keyspace.Drop()
	fmt.Println("")
	fmt.Println("Keyspace dropped")

}
