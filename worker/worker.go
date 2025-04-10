package main

import (
	"context"
	"flag"
	"log"
	"time"

	pb "client/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
)

func main() {
	flag.Parse()
	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	c := pb.NewControllerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.WorkerListen(ctx, &pb.ListenRequest{QueueName: "queue1"})
	if err != nil {
		log.Fatalf("could not send job: %v", err)
	}

	for {
		p, err := r.Recv()

		if err != nil {
			break
		}

		time.Sleep(10)
		log.Println("{}", p.Item)
	}

}
