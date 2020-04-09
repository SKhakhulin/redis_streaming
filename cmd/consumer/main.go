package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/redis_streaming/pkg/config"
	"github.com/redis_streaming/pkg/event"
	"os"

)

// TODO: use struct from target project
const (
	GroupName = "testGroup"
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

	events := eventFetcher(client, consumerName)
	consumeEvents(client, events)

	quit := make(chan bool)
	<-quit
}


func eventFetcher(client *redis.Client, consumerName string) chan event.Event {
	c := make(chan event.Event, 50)
	go func() {
		for {
			func() {
				rr, err := client.XReadGroup(
					&redis.XReadGroupArgs{
						Consumer: consumerName,
						Group:    GroupName,
						Streams:  []string{OrderStream, ">"},
				}).Result()
				if err != nil {
					panic(err)
				}
				for _, stream := range rr {
					for _, message := range stream.Messages {
						t := message.Values["type"].(string)
						e, err := event.New(event.Type(t))
						if err != nil {
							panic(err)
						}
						err = e.UnmarshalBinary([]byte(message.Values["data"].(string)))
						if err != nil {
							// TODO: Choose strategy
							client.XDel("orders", message.ID)
							fmt.Printf("fail to unmarshal event:%v\n", message.ID)
							return
						}
						e.SetID(message.ID)
						c <- e
					}
				}
			}()
		}
	}()
	return c
}

func consumeEvents(client *redis.Client, events chan event.Event){
 	go func() {
		for {
			var IDs []string
			var items []event.Event
			items = append(items, <-events)

			remains := 50

		Remaining:
			for i := 0; i < remains; i++ {
				select {
					case item := <-events:
						items = append(items, item)
						IDs = append(IDs, item.GetID())
					default:
						break Remaining
				}
			}

			fmt.Printf("process events: \n%v start: %v\n\n", len(items), IDs)
			if len(IDs) > 0 {
				_, err := client.XAck(OrderStream, GroupName, IDs...).Result()
				if err != nil {
					panic(err)
				}
			}
		}
	}()
}
//XPENDING order testGroup

func newRedisClient(cfg *config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	_, err := client.Ping().Result()
	return client, err

}
