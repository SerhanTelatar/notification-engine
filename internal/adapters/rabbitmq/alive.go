package rabbitmq

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Alive is a readiness check on the broker connection: it fails once the connection is closed
// (heartbeat timeout, broker restart or network loss).
func Alive(conn *amqp.Connection) func(context.Context) error {
	return func(context.Context) error {
		if conn.IsClosed() {
			return errors.New("rabbitmq connection closed")
		}
		return nil
	}
}
