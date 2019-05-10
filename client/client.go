package client

import (
	"encoding/json"
	"time"

	"github.com/go-redis/redis"
	"github.com/rs/xid"

	"sirfilip/workless/queue"
)

type Client struct {
	client *redis.Client
}

func (self *Client) Schedule(q string, worker string, duration int, payload ...interface{}) error {
	jobID := xid.New().String()
	jobSpec := &queue.JobSpec{
		ID:          jobID,
		Name:        worker,
		Payload:     payload,
		Duration:    duration,
		ScheduledAt: time.Now(),
	}
	jobSpecJSON, err := json.Marshal(jobSpec)
	if err != nil {
		return err
	}
	err = self.client.Set(queue.JobID(q, jobID), string(jobSpecJSON), 0).Err()
	if err != nil {
		return err
	}
	err = self.client.Set(queue.JobLockID(q, jobID), jobID, time.Duration(duration)*time.Second).Err()
	if err != nil {
		return err
	}
	return self.client.LPush(queue.PendingQueue(q), jobID).Err()
}

func (self *Client) ScheduleAt(q string, worker string, duration int, scheduledAt time.Time, payload ...interface{}) error {
	jobID := xid.New().String()
	jobSpec := &queue.JobSpec{
		ID:          jobID,
		Name:        worker,
		Payload:     payload,
		Duration:    duration,
		ScheduledAt: scheduledAt,
	}
	jobSpecJSON, err := json.Marshal(jobSpec)
	if err != nil {
		return err
	}
	err = self.client.Set(queue.JobID(q, jobID), string(jobSpecJSON), 0).Err()
	if err != nil {
		return err
	}
	err = self.client.Set(queue.JobLockID(q, jobID), jobID, time.Duration(duration)*time.Second).Err()
	if err != nil {
		return err
	}
	return self.client.LPush(queue.PendingQueue(q), jobID).Err()
}

func New(client *redis.Client) *Client {
	return &Client{client}
}
