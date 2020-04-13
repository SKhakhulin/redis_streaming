package main

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/redis_streaming/pkg/config"
	"github.com/redis_streaming/pkg/event"
	"os"
	"time"
)

// TODO: use struct from target project
const (
	GroupName = "testGroup"
	OrderStream = "order"
)


func main() {
	cfg := config.New()
	events := make(chan event.Event, 1000)

	client, err := newRedisClient(cfg)
	if err != nil {
		panic(err)
	}

	consumerName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	eventFetcher(client, consumerName, events)
	pendingEventFetcher(client, consumerName, events)

	consumeEvents(client, events)

	quit := make(chan bool)
	<-quit
}

func pendingEventFetcher(client *redis.Client, consumerName string, c chan event.Event)  {
	go func() {
		for {
			rr, err := client.XPending(OrderStream, GroupName).Result()
			if err != nil {
				panic(err)
			}

			for consumer := range rr.Consumers {
				rr, err := client.XPendingExt(
					&redis.XPendingExtArgs{
						Stream:   OrderStream,
						Group:    GroupName,
						Start:    "-",
						End:      "+",
						Count:    100,
						Consumer: consumer,
					}).Result()
				if err != nil {
					panic(err)
				}

				var IDs []string
				for item := range rr {
					if rr[item].Idle > 2*time.Minute {
						IDs = append(IDs, rr[item].ID)
					}
				}
				if len(IDs) > 0 {
					strCMD := client.XClaim(
						&redis.XClaimArgs{
							Stream:   OrderStream,
							Group:   GroupName,
							Consumer: consumerName,
							Messages: IDs,
						})
					rl, err := strCMD.Result()
					if err != nil {
						panic(err)
					}

					for _, message := range rl {
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

					fmt.Print(rl)
				}
			}

			time.Sleep(time.Minute)
		}
	}()
}

func eventFetcher(client *redis.Client, consumerName string, c chan event.Event)  {
	go func() {
		for {
			func() {
				rr, err := client.XReadGroup(
					&redis.XReadGroupArgs{
						Consumer: consumerName,
						Group:    GroupName,
						Count: 1,
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
}

func consumeEvents(client *redis.Client, events chan event.Event){
	go func() {
		var IDs []string
		var items []event.Event

		for {
			if len(items) < 50 {
				item := <-events
				items = append(items, item)
				IDs = append(IDs, item.GetID())
			} else {
				if len(items) > 0 {
					fmt.Printf("process events: %v IDs: %v\n", len(items), IDs)
				}
				if len(IDs) > 0 {
					n, err := client.XAck(OrderStream, GroupName, IDs...).Result()
					if err != nil {
						panic(err)
					}
					fmt.Printf("XAck: %v\n", n)
				}

				IDs = []string{}
				items = []event.Event{}
			}
		}
	}()
}


func newRedisClient(cfg *config.Config) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%v:%v", cfg.Redis.Host, cfg.Redis.Port),
		Password: cfg.Redis.Password,
		DB:       cfg.Redis.DB,
	})

	_, err := client.Ping().Result()
	return client, err

}
