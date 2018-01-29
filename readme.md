# Infrastructure Worker

Exampleo of a Golang backend construct using (currently) rabbitmq for message delivery.  Utilizes naemono/worker-pool for concurrency/threading.

## Configuration Environment Variables
The configuration is fully defined by environment variables listed below.  You can also find the defaults in `backend/config.go`

* INFWORKER_ENVIRONMENT
* INFWORKER_MAXWORKERS
* INFWORKER_RABBITURI
* INFWORKER_RABBITDEADEXCHANGE
* INFWORKER_RABBITDEADQUEUE
* INFWORKER_RABBITEXCHANGE
* INFWORKER_RABBITEXCHANGETYPE
* INFWORKER_RABBITQUEUE
* INFWORKER_RABBITROUTINGKEY
* INFWORKER_RABBITCONSUMERLIFETIME

## Installation

Ensure golang is installed.
git
`go install github.com/naemono/infrastructure-worker`

## Building

`go build .`

## Running

Run rabbitmq via docker compose
`docker-compose up -d`

Run the worker
`./infrastructure-worker`

## Sending test messages

Use the [rabbitmq mgmt console](http://localhost:15672/)

Make sure you set the `Properties` `content_type=application/json`

## Example message

```
{
  "id": "012345",
  "payload": "test"
}
```
