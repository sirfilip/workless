package main

import (
	"fmt"
	"os"
	"os/signal"

	"sirfilip/workless/queue"

	"github.com/go-redis/redis"
)

func helloWorld(args ...interface{}) error {
	who := "world"
	if len(args) > 0 {
		var ok bool
		who, ok = args[0].(string)
		if !ok {
			return fmt.Errorf("Failed to get the first arg: %v", args[0])
		}
	}
	fmt.Printf("Hello %s!\n", who)
	return nil
}

func main() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	queue.RegisterWorker("Hello", helloWorld)
	jobQueue := queue.NewJobQueue("hello", 100, &redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})
	jobQueue.Work()

	<-c
	jobQueue.Stop()
}
