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

	r, err := c.ClientAddItem(ctx, &pb.AddJobRequest{IsPeriodic: false, StartTime: -1, EndTime: -1, Interval: -1})
	if err != nil {
		log.Fatalf("could not send job: %v", err)
	} else {
		log.Println(r.Success)
	}

}
