package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/redis_streaming/pkg/config"
	"os"

)



const (
	Count = 50
	GroupName ="testGroup"
	OrderStream = "order"
)


func main() {
	cfg := config.New()

	client, err := newRedisClient(cfg)
	if err != nil {
		panic(err)
	}

	consumerName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	events := eventFetcher(client, Count, consumerName)
	consumeEvents(client, events)

	quit := make(chan bool)
	<-quit
}


func eventFetcher(client *redis.Client, count int64, consumerName string) chan redis.XMessage {
	c := make(chan redis.XMessage, 10)
	go func() {
		for {
			func() {
				rr, err := client.XReadGroup(
						&redis.XReadGroupArgs{
						Group:    GroupName,
						Count:    count,
						Consumer: consumerName,
						Streams:  []string{OrderStream, ">"},
				}).Result()
				if err != nil {
					panic(err)
				}

				for _, stream := range rr {
					for _, message := range stream.Messages {
						//TODO: UnmarshalBinary
						c <- message
					}
				}
			}()
		}
	}()
	return c
}

func consumeEvents(client *redis.Client, events chan redis.XMessage){
	fmt.Printf("XAck:%v\n", events)
	//for {

	//	select {
	//	case e := <-events:
	//		fmt.Printf("XAck:%v\n", e)
	//		//var IDs []string
	//		//for message := range events {
	//		//	IDs = append(IDs, message.ID)
	//		//}
	//		//fmt.Printf("IDs:%v\n", IDs)
	//		//
	//		//n, err := client.XAck(OrderStream, GroupName, IDs...).Result()
	//		//
	//		//if err != nil {
	//		//	panic(err)
	//		//}
	//		//fmt.Printf("XAck:%v\n", n)
	//	}
	//
	}
//}


func newRedisClient(cfg *config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	_, err := client.Ping().Result()
	return client, err

}
