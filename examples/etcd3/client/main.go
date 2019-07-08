package main

import (
	"context"
	"fmt"
	etcd "github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc"
	grpclb "grpc-lb"
	"grpc-lb/examples/proto"
	registry "grpc-lb/registry/etcd3"
	"log"
	"time"
)

func main() {
	etcdConfg := etcd.Config{
		Endpoints: []string{"http://127.0.0.1:2379"},
	}
	r := registry.NewResolver("/grpc-lb", "test", etcdConfg)
	b := grpclb.NewBalancer(r, grpclb.NewRoundRobinSelector())
	c, err := grpc.Dial("", grpc.WithInsecure(), grpc.WithBalancer(b), grpc.WithTimeout(time.Second*5))
	if err != nil {
		log.Printf("grpc dial: %scd", err)
		return
	}
	defer c.Close()

	client := proto.NewTestClient(c)
	fmt.Println("start")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, err := client.Say(ctx, &proto.SayReq{Content: "round robin"})
		cancel()
		if err != nil {
			log.Println("aa:", err)
			time.Sleep(time.Second)
			continue
		}
		time.Sleep(time.Second)
		log.Printf(resp.Content)
	}

}
