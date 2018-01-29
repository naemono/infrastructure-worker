package backend

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/naemono/infrastructure-worker/backend/amqp"
	"github.com/naemono/worker-pool/worker"
	log "github.com/sirupsen/logrus"
	libamqp "github.com/streadway/amqp"
	"github.com/xeipuuv/gojsonschema"
)

// Backender is an interface for a backend
type Backender interface {
	Setup() error
	Initialize() error
	GetMessages() (chan *worker.Message, chan error, error)
	Done() chan bool
	Quit(bool)
}

// Backend is a struct to allow multiple backends to message retrieval.
type Backend struct {
	config         Config
	messageChannel chan *worker.Message
	RabbitBackend  amqp.RabbitBackender
	done           chan bool
	quit           chan bool
	Errors         chan error
	MessageCount   int
}

// JobType represents the job to be run
type JobType struct {
	ClusterID string `json:"id,omitempty"`
	Method    string `json:"method", binding:"required"`
	Payload   []byte `json:"payload", binding:"required"`
}

// NewBackend returns a new Backend
func NewBackend() *Backend {
	config := NewConfig()
	return &Backend{
		RabbitBackend: amqp.NewRabbitBackend(true, config.MaxWorkers, config.RabbitURI,
			config.RabbitExchange, config.RabbitExchangeType,
			config.RabbitDeadExchange, config.RabbitDeadQueue,
			config.RabbitQueue, config.RabbitRoutingKey),
		config:         config,
		messageChannel: make(chan *worker.Message),
		quit:           make(chan bool, 1),
		done:           make(chan bool, 1),
		Errors:         make(chan error),
		MessageCount:   0,
	}
}

// Setup is intended to setup the initial connections to backends, but do nothing more.
func (b *Backend) Setup() error {
	if err := b.RabbitBackend.Connect(b.config.RabbitURI); err != nil {
		return fmt.Errorf("Error setting up backend: %s", err.Error())
	}
	return nil
}

// Initialize is intended to be used to run any initialization on backends, such as amqp,
// which require setting up queues/exchanges/etc.
func (b *Backend) Initialize() error {
	if !b.RabbitBackend.Connected() {
		return fmt.Errorf("Not connected: Run Setup prior to Initialize")
	}
	if err := b.RabbitBackend.BuildQueues(true); err != nil {
		message := fmt.Sprintf("Rabbitmq Build Queue Error: %s.\n", err)
		log.Errorf(message)
		err = fmt.Errorf(message)
		return err
	}
	return nil
}

// Quit sends the quit signal throughout the backend to stop all message passing.
func (b *Backend) Quit(val bool) {
	b.quit <- val
	return
}

// Done sends a done channel back to caller/client to notify when shutdown is complete.
func (b *Backend) Done() chan bool {
	return b.done
}

// GetMessages gets a message from the configured backend
func (b *Backend) GetMessages() (chan *worker.Message, chan error, error) {
	var (
		deliveries <-chan libamqp.Delivery
		err        error
		msg        worker.Message
	)
	if !b.RabbitBackend.Connected() {
		return nil, nil, fmt.Errorf("Not connected: Run Setup prior to Initialize")
	}
	if deliveries, err = b.getChannelFromAMQPBackend(); err != nil {
		log.Errorf("Error retrieving messages from Rabbitmq: %s", err)
		return nil, nil, err
	}
	go func() {
		for {
			select {
			case delivery := <-deliveries:
				// we have received a work request.
				go func() {
					if msg, err = b.processAmqpDelivery(delivery); err != nil {
						log.Errorf("Error processing message from Rabbitmq: %s", err)
						delivery.Nack(false, false)
						b.Errors <- fmt.Errorf("Failure processing message, error: %s", err.Error())
						b.MessageCount++
					} else {
						log.Debugf("Acknowledging message")
						delivery.Ack(false)
						log.Debugf("Sending message down messageChannel")
						b.messageChannel <- &msg
						b.MessageCount++
					}
				}()
			case <-b.quit:
				// we have received a signal to stop
				log.Infof("Received stop signal")
				log.Infof("Sending done signal")
				b.done <- true
				return
			default:
				log.Debugf("in delivery loop in GetMessages...")
				time.Sleep(1 * time.Second)
			}
		}
	}()
	return b.messageChannel, b.Errors, nil
}

// ** Rabbitmq/AMQP specific Functions **
// getChannelFromAMQPBackend gets a delivery channel from the amqp backend.
func (b *Backend) getChannelFromAMQPBackend() (<-chan libamqp.Delivery, error) {
	var (
		deliveries <-chan libamqp.Delivery
		err        error
	)
	if deliveries, err = b.RabbitBackend.Channel().Consume(
		b.config.RabbitQueue, // name
		"",                   // consumerTag,
		false,                // noAck
		false,                // exclusive
		false,                // noLocal
		false,                // noWait
		nil,                  // arguments
	); err != nil {
		return nil, fmt.Errorf("Queue Consume: %s", err)
	}
	return deliveries, err
}

// processAmqpDelivery takes an amqp delivery struct, and translates it into a message type.
func (b *Backend) processAmqpDelivery(d libamqp.Delivery) (worker.Message, error) {
	var (
		workType *JobType
		result   *gojsonschema.Result
		err      error
	)

	schema := gojsonschema.NewStringLoader(`{
	    "title": "Job",
	    "type": "object",
	    "properties": {
	        "id": {
						  "description": "cluster id",
	            "type": "string"
	        },
	        "method": {
						  "description": "method",
	            "type": "string"
	        },
	        "payload": {
	            "description": "payload",
	            "type": "string"
	        }
	    },
	    "required": ["method", "payload"]
	}`)
	workType = &JobType{}

	log.Debugf(fmt.Sprintf("Got message with body of '%s'.", string(d.Body)))

	// TODO json marshal worker.Job here
	if err := json.Unmarshal(d.Body, workType); err != nil {
		log.Error(fmt.Sprintf("Failure unmarshalling '%s'.", string(d.Body)))
		d.Nack(false, false)
		return worker.Message{}, err
	}
	log.Debugf("No failure unmarshalling, workType: ", workType)
	document := gojsonschema.NewGoLoader(workType)
	log.Debugf("Checking schema of message...")
	if result, err = gojsonschema.Validate(schema, document); err != nil {
		log.Error(fmt.Sprintf("Failure verifying json '%s', error: '%s'", string(d.Body), err.Error()))
		d.Nack(false, false)
		return worker.Message{}, err
	}
	log.Debugf("Result of validation, errors: %s", result.Errors())
	if !result.Valid() {
		return worker.Message{}, fmt.Errorf("Validation Errors: %s", result.Errors())
	}

	message := worker.Message{
		Method:    workType.Method,
		ClusterID: workType.ClusterID,
		Payload:   workType.Payload,
	}
	return message, nil
}

// ** End Rabbitmq/AMQP specific Functions **
