package amqp

import (
	"fmt"
	"strings"
	"testing"

	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
)

var (
	RB                       *RabbitBackend
	channelCalled            bool
	closeCalled              bool
	consumeCalled            bool
	exchangeDeclareCalled    bool
	queueDeclareCalled       bool
	qosCalled                bool
	channelError             bool
	queueBindCalled          bool
	exchangeDeclareError     bool
	deadExchangeDeclareError bool
	queueDeclareError        bool
	deadQueueDeclareError    bool
	queueBindError           bool
	deadQueueBindError       bool
)

func resetVars() {
	RB = NewRabbitBackend(true, 1, "testuri", "testexchange", "testexchangetype", "testdeadexchange", "deadqueue", "queuename", "routingkey")
	channelCalled = false
	closeCalled = false
	consumeCalled = false
	exchangeDeclareCalled = false
	queueDeclareCalled = false
	qosCalled = false
	queueBindCalled = false
	channelError = false
	exchangeDeclareError = false
	deadExchangeDeclareError = false
	deadQueueDeclareError = false
	queueDeclareError = false
	queueBindError = false
	deadQueueBindError = false
}

type TestRabbitConnection struct{}

type TestRabbitChannel struct{}

func (c *TestRabbitConnection) NotifyClose(chan *amqp.Error) chan *amqp.Error {
	return make(chan *amqp.Error)
}

func (c *TestRabbitConnection) Channel() (*amqp.Channel, error) {
	channelCalled = true
	if channelError {
		return &amqp.Channel{}, fmt.Errorf("Channel error")
	}
	return &amqp.Channel{}, nil
}

func (c *TestRabbitConnection) Close() error {
	closeCalled = true
	return nil
}

func (c *TestRabbitChannel) Consume(string, string, bool, bool, bool, bool, amqp.Table) (<-chan amqp.Delivery, error) {
	consumeCalled = true
	return make(chan amqp.Delivery), nil
}

func (c *TestRabbitChannel) ExchangeDeclare(name string, _ string, _ bool, _ bool, _ bool, _ bool, _ amqp.Table) error {
	exchangeDeclareCalled = true
	if strings.Contains(name, "testdeadexchange") && deadExchangeDeclareError {
		return fmt.Errorf("DeadExchangeDeclare error")
	} else if strings.Contains(name, "testexchange") && exchangeDeclareError {
		return fmt.Errorf("ExchangeDeclare error")
	}
	return nil
}

func (c *TestRabbitChannel) QueueDeclare(name string, _ bool, _ bool, _ bool, _ bool, _ amqp.Table) (amqp.Queue, error) {
	queueDeclareCalled = true
	if strings.Contains(name, "queuename") && queueDeclareError {
		return amqp.Queue{}, fmt.Errorf("QueueDeclare error")
	} else if strings.Contains(name, "deadqueue") && deadQueueDeclareError {
		return amqp.Queue{}, fmt.Errorf("DeadQueueDeclare error")
	}
	return amqp.Queue{}, nil
}

func (c *TestRabbitChannel) Qos(int, int, bool) error {
	qosCalled = true
	return nil
}

func (c *TestRabbitChannel) QueueBind(name string, _ string, _ string, _ bool, _ amqp.Table) error {
	queueBindCalled = true
	if (strings.Compare(name, "queuename") == 0) && queueBindError {
		return fmt.Errorf("QueueBind error")
	} else if strings.Compare(name, "deadqueue") == 0 && deadQueueBindError {
		return fmt.Errorf("DeadQueueBind error")
	}
	return nil
}

func init() {
	RB = NewRabbitBackend(true, 1, "testuri", "testexchange", "testexchangetype", "testdeadexchange", "deadqueue", "queuename", "routingkey")
}

func TestNewRabbitBackend(t *testing.T) {
	resetVars()
	assert.NotNil(t, RB)
}

func TestChannel(t *testing.T) {
	resetVars()
	ch := RB.Channel()
	assert.Nil(t, ch)
}

func TestConnect(t *testing.T) {
	resetVars()
	err := RB.Connect(RB.URI)
	assert.NotNil(t, err)
}

func TestConnected(t *testing.T) {
	resetVars()
	assert.False(t, RB.Connected())
}

func TestBuildQueuesFails(t *testing.T) {
	resetVars()
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
}

func TestBuildQueues(t *testing.T) {
	resetVars()
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.Nil(t, err)
	assert.True(t, qosCalled)
	assert.True(t, exchangeDeclareCalled)
	assert.True(t, queueDeclareCalled)
	assert.True(t, queueBindCalled)
}

func TestBuildQueuesDeadExchDeclError(t *testing.T) {
	resetVars()
	deadExchangeDeclareError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Dead Exchange declare error: DeadExchangeDeclare error.\n")
	assert.True(t, exchangeDeclareCalled)
}

func TestBuildQueuesExchDeclError(t *testing.T) {
	resetVars()
	exchangeDeclareError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Exchange declare error: ExchangeDeclare error.\n")
	assert.True(t, exchangeDeclareCalled)
}

func TestDeadQueueDeclareError(t *testing.T) {
	resetVars()
	deadQueueDeclareError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Dead Queue declare error: DeadQueueDeclare error.\n")
	assert.True(t, queueDeclareCalled)
}

func TestQueueBindError(t *testing.T) {
	resetVars()
	queueBindError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Queue bind error: QueueBind error.\n")
	assert.True(t, queueBindCalled)
}

func TestQueueDeclareError(t *testing.T) {
	resetVars()
	queueDeclareError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Rabbitmq Queue Declare error: QueueDeclare error")
	assert.True(t, queueDeclareCalled)
}

func TestDeadQueueBindError(t *testing.T) {
	resetVars()
	deadQueueBindError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = &TestRabbitChannel{}
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "Rabbitmq Exchange dead queue bind error: DeadQueueBind error")
	assert.True(t, queueDeclareCalled)
}

// func TestBuildQueuesNoChannel(t *testing.T) {
// 	resetVars()
// 	RB.connection = &TestRabbitConnection{}
// 	err := RB.BuildQueues(true)
// 	assert.Nil(t, err)
// }

func TestBuildQueuesChannelError(t *testing.T) {
	resetVars()
	channelError = true
	RB.connection = &TestRabbitConnection{}
	RB.channel = nil
	err := RB.BuildQueues(true)
	assert.NotNil(t, err)
}
