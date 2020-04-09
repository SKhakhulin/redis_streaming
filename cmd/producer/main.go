package main

import (
	"fmt"
	"github.com/redis_streaming/pkg/config"
	"math/rand"
	"os"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/redis_streaming/pkg/event"
)

// TODO: use struct from target project
const (
	Count = 500
	MaxCount = 50
	MaxUserIDRange = 10000
	GroupName ="testGroup"
	OrderStream = "order"
)

func main() {
	cfg := config.New()
	client, err := newRedisClient(cfg)
	if err != nil {
		panic(err)
	}

	CreateGroup(client, OrderStream, GroupName)
	GenerateEvents(client, Count)

	quit := make(chan bool)
	<-quit
}

func produceMessage(client *redis.Client)  {
	itemID := []string{"cpu", "ram", "hdd", "ssd"}[rand.Intn(4)]
	userID := int64(rand.Intn(MaxUserIDRange))
	count := uint(rand.Intn(MaxCount))

	strCMD := client.XAdd(&redis.XAddArgs{
		Stream: OrderStream,
		Values: map[string]interface{}{
			"type": string(event.OrderType),
			"data": &event.OrderEvent{
				Base: &event.Base{
					Type: event.OrderType,
				},
				UserID:         userID,
				ItemQuantities: []uint{count},
				ItemIDs:        []string{itemID},
			},
		},
	})

	_, err := strCMD.Result()

	if err != nil {
		fmt.Printf("test error:%v\n", err)
	}

}

func CreateGroup(client *redis.Client, orderStream string, groupName string) {
	strCMD := client.XGroupCreateMkStream(orderStream, groupName, "0")

	newID, err := strCMD.Result()

	if err != nil {
		fmt.Printf("CreateGroup error:%v\n", err)
		os.Exit(3)
	} else {
		fmt.Printf("CreateGroup %v\n", newID)
	}
}

func GenerateEvents(client *redis.Client, count int) {
	go func() {
		for {
			for i := 0; i < count; i++ {
				produceMessage(client)
			}
			time.Sleep(60 * time.Millisecond)
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
