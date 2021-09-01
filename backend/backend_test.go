package backend

import (
	"fmt"
	"testing"
	"time"

	"github.com/naemono/infrastructure-worker/backend/amqp"
	"github.com/naemono/worker-pool/worker"
	log "github.com/sirupsen/logrus"
	libamqp "github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var (
	connected         = true
	connectCalled     = false
	buildQueuesCalled = false
	testFailures      = false
	createBadMessages = false
	invalidMessages   = false
	makeDeliveries    = true
)

type TestBackend struct {
	config         Config
	messageChannel chan worker.Message
}

type TestRabbitBackend struct {
	connected bool
}

type TestRabbitChannel struct{}

func NewTestRabbitBackend() TestRabbitBackend {
	return TestRabbitBackend{
		connected: true,
	}
}

// GetMessages gets a message from the configured backend
func (b TestBackend) GetMessages() (chan *worker.Message, error) {
	wmchan := make(chan *worker.Message, 1)
	return wmchan, nil
}

func (b TestBackend) getChannelFromAMQPBackend() (<-chan libamqp.Delivery, error) {
	log.Infof("Sending message in getChannel")
	deliveries := make(chan libamqp.Delivery, 1)
	deliveries <- libamqp.Delivery{
		ContentType: "application/json",
		Exchange:    "test",
		RoutingKey:  "test",
		Body: []byte(`{
			"id": "01234",
			"method": "get",
			"payload": "test"}`),
	}
	return deliveries, nil
}

func (b TestBackend) processAmqpDelivery(d libamqp.Delivery) (worker.Message, error) {
	message := worker.Message{}
	return message, nil
}

func (r TestRabbitBackend) Channel() amqp.RabbitChanneller {
	return &TestRabbitChannel{}
}

func (r TestRabbitBackend) Connect(uri string) error {
	connectCalled = true
	if testFailures {
		return fmt.Errorf("Test Failure")
	}
	return nil
}

func (r TestRabbitBackend) Connected() bool {
	return connected
}

func (r TestRabbitBackend) BuildQueues(reliable bool) error {
	buildQueuesCalled = true
	if testFailures {
		return fmt.Errorf("Test Failure")
	}
	return nil
}

func (t TestRabbitChannel) Consume(string, string, bool, bool, bool, bool, libamqp.Table) (<-chan libamqp.Delivery, error) {
	log.Infof("In test consume")
	if !testFailures {
		log.Infof("after test failures")
		if createBadMessages {
			log.Infof("after createBadMessages")
			deliveries := make(chan libamqp.Delivery, 10)
			go func() {
				log.Infof("makeDeliveries: %b", makeDeliveries)
				if makeDeliveries {
					log.Infof("Making bad messages")
					for i := 0; i < 10; i++ {
						log.Infof("Made %d messages", i)
						if !invalidMessages {
							log.Infof("Making invalid json message")
							deliveries <- libamqp.Delivery{
								ContentType: "application/json",
								Body:        []byte(`{"bad": "message"}`),
							}
						} else {
							log.Infof("Making invalid message")
							deliveries <- libamqp.Delivery{
								ContentType: "application/json",
								Body:        []byte(`really bad`),
							}
						}
					}
				}
				return
			}()
			time.Sleep(1000 * time.Millisecond)
			log.Infof("Returning from Consume")
			return deliveries, nil
		}
		log.Infof("making deliveries of size 10")
		deliveries := make(chan libamqp.Delivery, 10)
		go func() {
			log.Infof("makeDeliveries: %b", makeDeliveries)
			if makeDeliveries {
				for i := 0; i < 10; i++ {
					log.Infof("Made %d messages", i)
					deliveries <- libamqp.Delivery{
						ContentType: "application/json",
						Body: []byte(`{
							"id": "01234",
							"method": "get",
							"payload": "test"}`),
					}
				}
			}
			return
		}()
		time.Sleep(1000 * time.Millisecond)
		return deliveries, nil
	}
	return nil, fmt.Errorf("Test Failure")
}

func (t TestRabbitChannel) ExchangeDeclare(string, string, bool, bool, bool, bool, libamqp.Table) error {
	return nil
}
func (t TestRabbitChannel) QueueDeclare(string, bool, bool, bool, bool, libamqp.Table) (libamqp.Queue, error) {
	return libamqp.Queue{}, nil
}
func (t TestRabbitChannel) Qos(int, int, bool) error {
	return nil
}
func (t TestRabbitChannel) QueueBind(string, string, string, bool, libamqp.Table) error {
	return nil
}

func TestNewBackend(t *testing.T) {
	backend := NewBackend()

	assert.NotNil(t, backend)
}

func TestBackendSetup(t *testing.T) {
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	connectCalled = false
	assert.Nil(t, backend.Setup())
	assert.True(t, connectCalled)
}

func TestBackendInitialize(t *testing.T) {
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	buildQueuesCalled = false
	assert.Nil(t, backend.Initialize())
	assert.True(t, buildQueuesCalled)
}

func TestBackendInitializeFailure(t *testing.T) {
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	buildQueuesCalled = false
	testFailures = true
	error := backend.Initialize()
	assert.NotNil(t, error)
	assert.True(t, buildQueuesCalled)
	assert.Equal(t, "Rabbitmq Build Queue Error: Test Failure.\n", error.Error(), fmt.Sprintf("'%s' is supposed to equal '%s'", error.Error(), "Rabbitmq Build Queue Error: Test Failure.\n"))
}

func TestBackendInitializeWhenNotConnected(t *testing.T) {
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()
	connected = false
	buildQueuesCalled = false
	defer func() {
		connected = true
	}()
	testFailures = false
	error := backend.Initialize()
	assert.NotNil(t, error)
}

func TestGetChannelFromAMQPBackend(t *testing.T) {
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	buildQueuesCalled = false
	testFailures = false
	deliveries, error := backend.getChannelFromAMQPBackend()
	assert.Nil(t, error)
	assert.NotNil(t, deliveries)

	buildQueuesCalled = false
	testFailures = true
	deliveries, error = backend.getChannelFromAMQPBackend()
	assert.NotNil(t, error)
	assert.Nil(t, deliveries)
}

func TestGetMessagesWithoutMessages(t *testing.T) {
	var (
		err error
	)
	log.Infof("Test 1")
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	makeDeliveries = false
	testFailures = false
	_, _, err = backend.GetMessages()
	assert.Nil(t, err)
}

func TestGetMessagesWithMessages(t *testing.T) {
	var (
		err      error
		errors   chan error
		messages chan *worker.Message
	)
	log.Infof("Test 2")
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()
	makeDeliveries = true
	testFailures = false
	messages, errors, err = backend.GetMessages()
	assert.Nil(t, err)
	exit := false
	for {
		select {
		case msg := <-messages:
			log.Infof("Got messages from backend: %s", msg)
		case err := <-errors:
			log.Infof("Got error: %s", err.Error())
		default:
			if backend.MessageCount == 10 {
				log.Infof("Breaking test loop")
				backend.quit <- true
				exit = true
				<-backend.done
				break
			}
			log.Infof("MessageCount: %d", backend.MessageCount)
			time.Sleep(1 * time.Second)
		}
		if exit {
			break
		}
	}
}

func TestGetMessagesWithFailures(t *testing.T) {
	var (
		err error
	)
	log.Infof("Test 3")
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()
	makeDeliveries = false
	testFailures = true
	_, _, err = backend.GetMessages()
	assert.NotNil(t, err)
}

func TestGetBadJsonMessages(t *testing.T) {
	var messages chan *worker.Message
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	makeDeliveries = true
	testFailures = false
	createBadMessages = true
	exit := false
	_, errors, err := backend.GetMessages()
	assert.Nil(t, err)
	errorCount := 0
	msgCount := 0
	for {
		select {
		case msg := <-messages:
			msgCount++
			log.Infof("Got messages from backend: %s", msg)
		case err := <-errors:
			errorCount++
			log.Infof("Got error: %s", err.Error())
		default:
			if backend.MessageCount == 10 {
				backend.quit <- true
				<-backend.done
				exit = true
				break
			}
			log.Infof("MessageCount: %d", backend.MessageCount)
			time.Sleep(1 * time.Second)
		}
		if exit {
			break
		}
	}
	assert.Exactly(t, 10, errorCount)
	assert.Exactly(t, 0, msgCount)
}

func TestGetBadMessages(t *testing.T) {
	var messages chan *worker.Message
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()

	makeDeliveries = true
	testFailures = false
	createBadMessages = true
	invalidMessages = true
	exit := false
	_, errors, err := backend.GetMessages()
	assert.Nil(t, err)
	errorCount := 0
	msgCount := 0
	for {
		select {
		case msg := <-messages:
			msgCount++
			log.Infof("Got messages from backend: %s", msg)
		case err := <-errors:
			errorCount++
			log.Infof("Got error: %s", err.Error())
		default:
			if errorCount == 10 {
				backend.quit <- true
				<-backend.done
				exit = true
				break
			}
			log.Infof("MessageCount: %d", backend.MessageCount)
			time.Sleep(1 * time.Second)
		}
		if exit {
			break
		}
	}
	assert.Exactly(t, 10, errorCount)
	assert.Exactly(t, 0, msgCount)
}

func TestGetMessagesWhenNotConnected(t *testing.T) {
	backend := NewBackend()
	backend.RabbitBackend = NewTestRabbitBackend()
	connected = false
	defer func() {
		connected = true
	}()
	makeDeliveries = false
	testFailures = false
	log.Errorf("About to get messages...")
	_, _, error := backend.GetMessages()
	assert.NotNil(t, error)
}
