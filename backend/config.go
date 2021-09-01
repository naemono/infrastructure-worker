package backend

import (
	"fmt"

	"github.com/kelseyhightower/envconfig"
)

// Config simply holds configuration items.
type Config struct {
	Environment            string `default:"development"`
	MaxWorkers             int    `default:"1"`
	RabbitURI              string `default:"amqp://devtools:devtools@localhost:5672/"`
	RabbitDeadExchange     string `default:"devtools.dead"`
	RabbitDeadQueue        string `default:"worker.dead"`
	RabbitExchange         string `default:"devtools"`
	RabbitExchangeType     string `default:"topic"`
	RabbitQueue            string `default:"worker"`
	RabbitRoutingKey       string `default:"devtools"`
	RabbitConsumerLifetime string `default:"0s"`
}

// NewConfig builds a new configuration struct.
func NewConfig() Config {
	var config Config
	err := envconfig.Process("infworker", &config)
	if err != nil {
		panic(fmt.Sprintf("Failure getting configuration: %s", err))
	}

	return config
}
