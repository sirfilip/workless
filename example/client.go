package main

import "github.com/go-redis/redis"
import "sirfilip/workless/client"

func main() {
	redisDB := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	c := client.New(redisDB)
	names := []string{
		"John",
		"Jane",
		"World",
		"Filip",
		"Another one",
		"Blue",
		"Red",
		"Yellow",
	}
	for _, name := range names {
		err := c.Schedule("hello", "Hello", name)
		if err != nil {
			panic(err)
		}
	}
}
