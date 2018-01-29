package main

import (
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"

	infbackend "github.com/naemono/infrastructure-worker/backend"
	dispatch "github.com/naemono/worker-pool/dispatcher"
	"github.com/naemono/worker-pool/worker"
)

var (
	// MaxWorker is the max num of workers
	MaxWorker = os.Getenv("MAX_WORKERS")
	// MaxQueue is the max num of queues
	MaxQueue = os.Getenv("MAX_QUEUE")
	// Backend is the backend that is intended to handle multiple types of backends, such as amqp/sqs
	Backend infbackend.Backender
	// Dispatcher is the worker's dispatcher which handled passing messages to workers
	Dispatcher *dispatch.Dispatcher
)

//
// // worker.Message is the message type to be received
// type Message struct {
// 	Method    string `json: "Method"`
// 	ClusterID string `json: "ClusterID,omitempty"`
// 	// Intent here is to ensure json, or yaml
// 	// Example here https://mlafeldt.github.io/blog/teaching-go-programs-to-love-json-and-yaml/
// 	Payload []byte `json: "Payload"`
// }
//

func init() {
	if strings.Compare(MaxWorker, "") == 0 {
		MaxWorker = "10"
	}
	if strings.Compare(MaxQueue, "") == 0 {
		MaxQueue = "2"
	}
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.DebugLevel)
}

func main() {
	var (
		maxWorkers int
		err        error
		errChannel chan error
	)

	if maxWorkers, err = strconv.Atoi(MaxWorker); err != nil {
		log.Error("Error converting %s to Digit: %s", MaxWorker, err.Error())
	}
	Backend = infbackend.NewBackend()
	if err = Backend.Setup(); err != nil {
		log.Fatalf("Error setting up backend: %s", err.Error())
	}
	if err = Backend.Initialize(); err != nil {
		log.Fatalf("Error initializing backend: %s", err.Error())
	}
	log.Info("starting dispatchers and workers...")
	Dispatcher = dispatch.NewDispatcher(maxWorkers, workerFunc)
	Dispatcher.Run()

	defer Dispatcher.Stop()

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigs
		log.Infof("Received Signal: %d", sig)
		Backend.Quit(true)
		backendDone := Backend.Done()
		<-backendDone
		done <- true
	}()

	go func() {
		var (
			msgChannel chan *worker.Message
			err        error
		)
		if msgChannel, errChannel, err = Backend.GetMessages(); err != nil {
			log.Errorf("Error getting message from backend: %s", err)
			return
		}
		log.Debugf("Received msgChannel from backend")
		for {
			select {
			case m := <-msgChannel:
				log.Infof("Sending message to dispatcher: payload: %s", m.Payload)
				Dispatcher.SendJob(worker.Job{Payload: *m})
			case e := <-errChannel:
				log.Errorf("Error found during processing of message: %s", e.Error())
			}
		}
	}()

	log.Info("Will exit on ctrl-c")
	<-done
	log.Info("exiting")
}

// workerFunc is an example of a method that can be sent to worker-pool/dispatcher
// to handle, or begin handling types of jobs
func workerFunc(job worker.Job) error {
	log.Debugf("Got job with payload: %s", job.Payload.Payload)
	return nil
}
