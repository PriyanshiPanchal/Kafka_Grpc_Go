package main

import (
	"context"
	"fmt"
	// "log"
	// "os"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"github.com/segmentio/kafka-go"
)

const (
	topic         = "TestTopic"
	brokerAddress = "localhost:9092"
)

func main() {
	db, err:= sql.Open("mysql", "root:priya@1298@tcp(localhost:3306)/kafka_consumer")
	if err != nil {
		fmt.Println("Error creating DB:", err)
    	fmt.Println("To verify, db is:", db)
	}
	defer db.Close()
	fmt.Println("Successfully  Connected to MYSQl")
	
	err = db.Ping()
	if err != nil {
		fmt.Print(err.Error())
	}

	ctx := context.Background()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
		GroupID: "my-group",
	})
	
	for {
		// the `ReadMessage` method blocks until we receive the next event
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("could not read message " + err.Error())
		}
		fmt.Println("received: ", string(msg.Value))
		
		var user_name= string(msg.Value)
		stmt, err := db.Prepare("insert into message (username) values(?);")
		if err != nil {
			fmt.Print(err.Error())
		}
		_, err = stmt.Exec(user_name)

		if err != nil {
			fmt.Print(err.Error())
		}
	}
}