package main

import (
	"context"
	"Kafka_Program/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"fmt"
	"time"
	"github.com/segmentio/kafka-go"
)
type server struct{}

const (
	topic         = "TestTopic"
	brokerAddress = "localhost:9092"
)

func main(){
	listener, err:=net.Listen("tcp",":4040")
	if err!=nil{
		panic(err)
	}
	srv:=grpc.NewServer()
	proto.RegisterProducerServiceServer(srv, &server{})
	reflection.Register(srv)

	if e:=srv.Serve(listener); e!=nil{
		panic(err)
	}
}

func (s *server) Producer(ctx context.Context, request *proto.Request)(*proto.Response, error){
	a:= request.GetUsername()

	result:= a

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{brokerAddress},
		Topic:   topic,
	})

	err := w.WriteMessages(ctx, kafka.Message{
		Key: []byte(result),
		Value: []byte(result),
	})

	if err != nil {
		panic("could not write message " + err.Error())
	}

	fmt.Println("writes:", result)
	time.Sleep(time.Second)

	return &proto.Response{Result:"success"}, nil
}