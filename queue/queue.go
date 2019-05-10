package queue

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-redis/redis"
)

// Job type of job interface
type Job func(...interface{}) error

// JobSpec json serialilzed job information
type JobSpec struct {
	ID          string        `json:"id"`           // Job ID
	Name        string        `json:"name"`         // Name of the handler
	Duration    int           `json:"duration"`     // Duration in seconds
	Attempts    int           `json:"attempts"`     // Number of retries
	ScheduledAt time.Time     `json:"scheduled_at"` // Execute not sooner then
	ExecutedAt  time.Time     `json:"executed_at"`  // Last time this job was executed
	Payload     []interface{} `json:"payload"`      // The payload of the job
}

var workers map[string]Job

func init() {
	workers = make(map[string]Job)
}

// RegisterWorker introduce new worker into known workers queue
func RegisterWorker(name string, worker Job) {
	workers[name] = worker
}

// PendingQueue returns the key of the pending queue
func PendingQueue(queue string) string {
	return fmt.Sprintf("workless:pending:%s", queue)
}

// ProcessingQueue returns the key of the processing queue
func ProcessingQueue(queue string) string {
	return fmt.Sprintf("workless:processing:%s", queue)
}

// JobLockID returns the key of the lock
func JobLockID(queue string, id string) string {
	return fmt.Sprintf("workless:%s:locks:%s", queue, id)
}

// JobID returns the key of the job
func JobID(queue string, id string) string {
	return fmt.Sprintf("workless:%s:jobs:%s", queue, id)
}

// JobQueue is the job queue implementation
type JobQueue struct {
	name    string
	options *redis.Options
	buffer  chan *JobSpec
	done    chan int
	stop    chan int
}

func (self *JobQueue) Start() {
	log.Printf("Starting queue %v ...", self.name)
	for i := 0; i < cap(self.buffer); i++ {
		client := redis.NewClient(self.options)
		_, err := client.Ping().Result()
		if err != nil {
			panic(err)
		}
		go func(worker int, client *redis.Client) {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("Error: worker(%v) recover: %v", worker, err)
				}
			}()
			defer client.Close()
			for {
				select {
				case jobSpec := <-self.buffer:
					if job, ok := workers[jobSpec.Name]; ok {
						err := job(jobSpec.Payload...)
						if err != nil {
							// TODO: update stats
							log.Printf("Error while executing job: %v", err)
						} else {
							if err := client.Del(JobID(self.name, jobSpec.ID)).Err(); err != nil {
								log.Printf("Error removing job: %v", err)
							}
							if err := client.LRem(ProcessingQueue(self.name), 1, jobSpec.ID).Err(); err != nil {
								log.Printf("Error removing job from the processing list: %v", err)
							}
						}
					}
				case <-self.done:
					return
				default:
					if res, err := client.BRPopLPush(PendingQueue(self.name), ProcessingQueue(self.name), 10*time.Second).Result(); err != nil {
						if err != redis.Nil {
							log.Printf("Error while fetching next job: %v", err)
						}
						continue
					} else {
						if res == "" {
							continue
						}
						log.Printf("Got payload: %v", res)
						if payload, err := client.Get(JobID(self.name, res)).Result(); err != nil {
							log.Printf("Error while getting payload: %v", err)
						} else {
							jobSpec := &JobSpec{}
							err := json.Unmarshal([]byte(payload), jobSpec)
							if err != nil {
								// log the error
								log.Printf("Error while unmarshaling payload: %v", err)
							} else {
								log.Printf("Sending Job Spec: %v", jobSpec)
								self.buffer <- jobSpec
							}
						}
					}
				}
			}
		}(i, client)
	}
	log.Printf("Queue %v started", self.name)
}

// Stop stops the channel from processing jobs
func (self *JobQueue) Stop() {
	log.Printf("Stopping queue %v ...", self.name)
	for i := 0; i < cap(self.buffer); i++ {
		self.done <- 1
	}
	log.Print("Bye")
}

// NewJobQueue JobQueue constructor
func NewJobQueue(name string, capacity int, redisOptions *redis.Options) *JobQueue {
	buffer := make(chan *JobSpec, capacity)
	done := make(chan int, capacity)
	return &JobQueue{
		name:    name,
		options: redisOptions,
		buffer:  buffer,
		done:    done,
	}
}
