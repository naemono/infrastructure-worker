package amqp

import (
	"fmt"

	log "github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// RabbitConnectioner rabbit connection interface
type RabbitConnectioner interface {
	Channel() (*amqp.Channel, error)
	NotifyClose(chan *amqp.Error) chan *amqp.Error
}

// RabbitChanneller rabbit channel interface
type RabbitChanneller interface {
	Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error)
	ExchangeDeclare(string, string, bool, bool, bool, bool, amqp.Table) error
	QueueDeclare(string, bool, bool, bool, bool, amqp.Table) (amqp.Queue, error)
	Qos(int, int, bool) error
	QueueBind(string, string, string, bool, amqp.Table) error
}

// RabbitBackender is an interface to RabbitBackend
type RabbitBackender interface {
	Channel() RabbitChanneller
	Connect(string) error
	Connected() bool
	BuildQueues(bool) error
}

// RabbitBackend is a RabbitMQ backend.
type RabbitBackend struct {
	connection   RabbitConnectioner
	channel      RabbitChanneller
	tag          string
	connected    bool
	done         chan error
	URI          string
	Exchange     string
	ExchangeType string
	QueueName    string
	DeadExchange string
	DeadQueue    string
	Key          string
	Ctag         string
	maxWorkers   int
	queue        amqp.Queue
}

// NewRabbitBackend gives back a new rabbitmq backend
func NewRabbitBackend(reliable bool, maxWorkers int, uri string, exchange string,
	exchangeType string, deadExchange string, deadQueue string,
	queueName string, routingKey string) *RabbitBackend {
	r := &RabbitBackend{
		channel:      nil,
		connected:    false,
		connection:   nil,
		maxWorkers:   maxWorkers,
		done:         make(chan error),
		URI:          uri,
		Exchange:     exchange,
		ExchangeType: exchangeType,
		DeadExchange: deadExchange,
		DeadQueue:    deadQueue,
		QueueName:    queueName,
		Key:          routingKey,
		Ctag:         "infrastructure-worker",
	}

	return r
}

// Channel returns a rabbitmq channel
func (r *RabbitBackend) Channel() RabbitChanneller {
	return r.channel
}

// Connect connects to rabbitmq, and sets the connected flag
func (r *RabbitBackend) Connect(uri string) (err error) {
	r.connection, err = amqp.Dial(uri)
	if err == nil {
		r.connected = true
	}
	return err
}

// Connected returns whether rabbitmq is connected
func (r *RabbitBackend) Connected() bool {
	return r.connected
}

// BuildQueues builds out the necessary rabbitmq queues.
func (r *RabbitBackend) BuildQueues(reliable bool) (err error) {
	if r.connection == nil {
		return fmt.Errorf("Please Connect to Backend first with 'Connect'")
	}

	go func() {
		log.Debugf("About to call notifyclose")
		log.Infof("closing: %s", <-r.connection.NotifyClose(make(chan *amqp.Error)))
	}()

	if r.channel == nil {
		log.Debugf("Creating new channel..")
		if r.channel, err = r.connection.Channel(); err != nil {
			message := fmt.Sprintf("Channel Error: %s.\n", err)
			log.Infof(message)
			err = fmt.Errorf(message)
			return err
		}
	}

	// Limit consumer to 1 message at a time, for ease of implementation.
	log.Debugf("Setting qos")
	r.channel.Qos(r.maxWorkers, 0, false)

	log.Debugf("Calling ExchangeDeclare for dead exchange")
	if err = r.channel.ExchangeDeclare(
		r.DeadExchange, // name of the exchange
		r.ExchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		message := fmt.Sprintf("Dead Exchange declare error: %s.\n", err)
		log.Errorf(message)
		err = fmt.Errorf(message)
		return err
	}

	log.Debugf("Calling ExchangeDeclare for primary exchange")
	if err = r.channel.ExchangeDeclare(
		r.Exchange,     // name of the exchange
		r.ExchangeType, // type
		true,           // durable
		false,          // delete when complete
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		message := fmt.Sprintf("Exchange declare error: %s.\n", err)
		log.Errorf(message)
		err = fmt.Errorf(message)
		return err
	}

	queueArgs := map[string]interface{}{
		"x-dead-letter-exchange": r.DeadExchange,
	}

	log.Debugf("Calling QueueDeclare for dead queue")
	if _, err = r.channel.QueueDeclare(
		r.DeadQueue, // name of the queue
		true,        // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // noWait
		nil,         // arguments
	); err != nil {
		message := fmt.Sprintf("Dead Queue declare error: %s.\n", err)
		log.Errorf(message)
		err = fmt.Errorf(message)
		return err
	}

	log.Debugf("Calling QueueBind for dead queue")
	if err = r.channel.QueueBind(
		r.DeadQueue,    // name
		r.QueueName,    // key
		r.DeadExchange, // exchange
		false,          // noWait
		nil,            // args
	); err != nil {
		message := fmt.Sprintf("Rabbitmq Exchange dead queue bind error: %s", err)
		log.Errorf(message)
		return fmt.Errorf(message)
	}

	log.Debugf("Calling QueueDeclear for primary queue")
	if r.queue, err = r.channel.QueueDeclare(
		r.QueueName, // name of the queue
		true,        // durable
		false,       // delete when usused
		false,       // exclusive
		false,       // noWait
		queueArgs,   // arguments
	); err != nil {
		message := fmt.Sprintf("Rabbitmq Queue Declare error: %s", err)
		log.Errorf(message)
		return fmt.Errorf(message)
	}

	log.Debugf("Calling QueueBind for primary queue")
	if err = r.channel.QueueBind(
		r.QueueName, // name of the queue
		r.Key,       // bindingKey
		r.Exchange,  // sourceExchange
		false,       // noWait
		nil,         // arguments
	); err != nil {
		message := fmt.Sprintf("Queue bind error: %s.\n", err)
		log.Errorf(message)
		err = fmt.Errorf(message)
		return err
	}

	return err
}
