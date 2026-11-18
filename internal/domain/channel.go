// Package domain holds the notification model, its state machine and its validation rules. It
// depends only on the standard library (Constitution II).
package domain

import (
	"errors"
	"fmt"
)

// ErrInvalidChannel is returned for an unknown channel value.
var ErrInvalidChannel = errors.New("invalid channel")

// Channel is the delivery channel (data-model.md notifications.channel).
type Channel string

const (
	ChannelSMS   Channel = "sms"
	ChannelEmail Channel = "email"
	ChannelPush  Channel = "push"
)

// Channels lists every channel in a stable order.
var Channels = []Channel{ChannelSMS, ChannelEmail, ChannelPush}

// ParseChannel accepts exactly "sms", "email" or "push".
func ParseChannel(s string) (Channel, error) {
	switch c := Channel(s); c {
	case ChannelSMS, ChannelEmail, ChannelPush:
		return c, nil
	default:
		return "", fmt.Errorf("%w: %q (want sms, email or push)", ErrInvalidChannel, s)
	}
}

func (c Channel) String() string { return string(c) }
