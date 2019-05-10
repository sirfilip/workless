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
	ID         string        `json:"id"`          // Job ID
	Name       string        `json:"name"`        // Name of the handler
	Payload    []interface{} `json:"payload"`     // The payload of the job
	Status     int           `json:"status"`      // Status pending, processed failed
	ExecutedAt time.Time     `json:"executed_at"` // Last time this job was executed
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
	// TODO add catch all to make sure that the worker gorutine keeps running on error
	for i := 0; i < cap(self.buffer); i++ {
		client := redis.NewClient(self.options)
		_, err := client.Ping().Result()
		if err != nil {
			panic(err)
		}
		go func(client *redis.Client) {
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
							client.LRem(ProcessingQueue(self.name), 1, jobSpec.ID)
						}
					}
				case <-self.done:
					return
				default:
					if res, err := client.RPopLPush(PendingQueue(self.name), ProcessingQueue(self.name)).Result(); err != nil {
						// log.Printf("Error while fetching next job: %v", err)
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
		}(client)
	}
}

// Stop stops the channel from processing jobs
func (self *JobQueue) Stop() {
	for i := 0; i < cap(self.buffer); i++ {
		self.done <- 1
	}
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
