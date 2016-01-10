package main

import (
	"fmt"
	"log"
	"time"

	"github.com/kristoiv/gocqltable"
	"github.com/kristoiv/gocqltable/recipes"
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
		Password string `password`     // Use Tags to rename fields
		Active   bool   `cql:"active"` // If there are multiple tags, use `cql:""` to specify what the table column will be
		Created  time.Time
	}

	// Let's define and instantiate a table object for our user table
	userTable := struct {
		recipes.CRUD // If you looked at the base example first, notice we replaced this line with the recipe
	}{
		recipes.CRUD{ // Here we didn't replace, but rather wrapped the table object in our recipe, effectively adding more methods to the end API
			keyspace.NewTable(
				"users",           // The table name
				[]string{"email"}, // Row keys
				nil,               // Range keys
				User{},            // We pass an instance of the user struct that will be used as a type template during fetches.
			),
		},
	}

	// Lets create this table in our cassandra database
	err = userTable.Create()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Table created: users")

	// Now that we have a keyspace with a table in it: lets make a few rows! In the base example we had to write out the CQL manually, this time
	// around, however, we can insert entire User objects.

	// Lets instantiate a user object, set its values and insert it
	user1 := User{
		Email:    "1@example.com",
		Password: "123456",
		Active:   true,
		Created:  time.Now().UTC(),
	}
	err = userTable.Insert(user1)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("User inserted:", user1)

	// And again for our next user.
	user2 := User{
		Email:    "2@example.com",
		Password: "123456",
		Active:   true,
		Created:  time.Now().UTC(),
	}
	err = userTable.Insert(user2)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("User inserted:", user2)

	// And finally again for the third user.
	user3 := User{
		Email:    "3@example.com",
		Password: "123456",
		Active:   true,
		Created:  time.Now().UTC(),
	}
	err = userTable.Insert(user3)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("User inserted:", user3)

	// With our database filled up with users, lets query it and print out the results (containing all users in the database).
	rowset, err := userTable.List()
	fmt.Println("")
	fmt.Println("Fetched all from users:")
	for _, user := range rowset.([]*User) {
		// user := row.(*User)         // Our row variable is a pointer to "interface{}", and here we type assert it to a pointer to "User"
		fmt.Println("User: ", user) // Let's just print that
	}
	if err != nil {
		log.Fatalln(err)
	}

	// You can also fetch a single row, obviously
	row, err := userTable.Get("2@example.com")
	if err != nil {
		log.Fatalln(err)
	}
	user := row.(*User)
	fmt.Println("")
	fmt.Println("Fetched single row by email: ", user)

	// Lets update this user by changing his password
	user.Password = "654321"
	err = userTable.Update(user)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Updated user to:", user)

	// Lets delete user 3@example.com
	err = userTable.Delete(user3)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Deleted user:", user3)

	// Lets print the final list of users
	rowset, err = userTable.List()
	fmt.Println("")
	fmt.Println("Final list of users:")
	for _, user := range rowset.([]*User) {
		// user := row.(*User)         // Our row variable is a pointer to "interface{}", and here we type assert it to a pointer to "User"
		fmt.Println("User: ", user) // Let's just print that
	}
	if err != nil {
		log.Fatalln(err)
	}

	// Now lets get a little more advanced in our data modelling. Lets do a range-model with log items belonging to one of our users
	// and do range scans on them using the high-level API in gocqltable

	// First we need a structure to represent these log items
	type UserLog struct {
		Email string     // Row key part of our primary key
		Id    gocql.UUID // Range key part of our primary key. Will use UUID version 1 (timeuuid).
		Data  int        // The data we will log (in this case a integer value)
	}

	// Then we need a new table to hold these log items
	userLogTable := struct {
		recipes.CRUD
	}{
		recipes.CRUD{
			keyspace.NewTable(
				"user_logs",
				[]string{"email"},
				[]string{"id"},
				UserLog{},
			),
		},
	}

	// Lets create this table in the database
	err = userLogTable.Create()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Table created: user_logs")

	// Then we populate it with example log data
	log1 := UserLog{
		Email: "1@example.com",
		Id:    gocql.TimeUUID(),
		Data:  1,
	}
	err = userLogTable.Insert(log1)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Log inserted:", log1)

	log2 := UserLog{
		Email: "2@example.com",
		Id:    gocql.TimeUUID(),
		Data:  2,
	}
	err = userLogTable.Insert(log2)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Log inserted:", log2)

	log3 := UserLog{
		Email: "2@example.com",
		Id:    gocql.TimeUUID(),
		Data:  3,
	}
	err = userLogTable.Insert(log3)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Log inserted:", log3)

	log4 := UserLog{
		Email: "2@example.com",
		Id:    gocql.TimeUUID(),
		Data:  4,
	}
	err = userLogTable.Insert(log4)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Log inserted:", log4)

	// Now finally lets do some range queries on our new table.
	rowset, err = userLogTable.Range().Fetch() // If we don't specify any ids in Range(ids...), then it will do a full Scan of the table
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Fetching all user logs:")
	for _, userLog := range rowset.([]*UserLog) {
		//userLog := row.(*UserLog)
		fmt.Println("UserLog: ", userLog)
	}

	rowset, err = userLogTable.Range("2@example.com").Fetch() // In this case we filter the result by row key and scan over all log items
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Fetching all user logs by user 2@example.com:")
	for _, userLog := range rowset.([]*UserLog) {
		// userLog := row.(*UserLog)
		fmt.Println("UserLog: ", userLog)
	}

	rowset, err = userLogTable.Range("2@example.com").MoreThanOrEqual("id", log3.Id).OrderBy("id DESC").Fetch()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Fetching all user logs by user 2@example.com, where id >= log3.Id:")
	for _, userLog := range rowset.([]*UserLog) {
		//userLog := row.(*UserLog)
		fmt.Println("UserLog: ", userLog)
	}

	rowset, err = userLogTable.Range("2@example.com").MoreThan("id", log3.Id).Fetch()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Fetching all user logs by user 2@example.com, where id > log3.Id:")
	for _, userLog := range rowset.([]*UserLog) {
		//userLog := row.(*UserLog)
		fmt.Println("UserLog: ", userLog)
	}

	rowset, err = userLogTable.Range("2@example.com").LessThan("id", log3.Id).Limit(10).Fetch()
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("")
	fmt.Println("Fetching all user logs by user 2@example.com, where id < log3.Id:")
	for _, userLog := range rowset.([]*UserLog) {
		// userLog := row.(*UserLog)
		fmt.Println("UserLog: ", userLog)
	}

	// Lets clean up after ourselves by dropping the keyspace.
	keyspace.Drop()
	fmt.Println("")
	fmt.Println("Keyspace dropped")

}
