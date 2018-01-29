package backend

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestNewWorker tests the NewWorker func
func TestNewConfig(t *testing.T) {

	c := NewConfig()

	assert.Equal(t, "development", c.Environment)
	assert.Equal(t, 1, c.MaxWorkers)
	assert.Equal(t, "amqp://devtools:devtools@localhost:5672/", c.RabbitURI)
	assert.Equal(t, "devtools.dead", c.RabbitDeadExchange)
	assert.Equal(t, "worker.dead", c.RabbitDeadQueue)
	assert.Equal(t, "devtools", c.RabbitExchange)
	assert.Equal(t, "topic", c.RabbitExchangeType)
	assert.Equal(t, "worker", c.RabbitQueue)
	assert.Equal(t, "devtools", c.RabbitRoutingKey)
	assert.Equal(t, "0s", c.RabbitConsumerLifetime)
	assert.NotNil(t, c)
}

func TestNewConfigPanics(t *testing.T) {
	os.Setenv("INFWORKER_MAXWORKERS", "asdf")
	assert.Panics(t, func() { _ = NewConfig() })
}
