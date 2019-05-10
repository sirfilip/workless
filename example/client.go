package main

import (
	"fmt"
	"sirfilip/workless/client"

	"github.com/go-redis/redis"
)

func main() {
	redisDB := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	c := client.New(redisDB)
	names := []string{}
	for i := 0; i < 1000000; i++ {
		names = append(names, fmt.Sprintf("Player num: %v", i))
	}
	for _, name := range names {
		err := c.Schedule("hello", "Hello", name)
		if err != nil {
			panic(err)
		}
	}
}
