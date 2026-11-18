package domain

import (
	"fmt"
	"net/mail"
	"regexp"
	"strings"
	"unicode/utf8"
)

// Per-channel limits (FR-003, data-model.md notifications.recipient and notifications.content).
const (
	SMSMaxChars       = 160     // Unicode code points
	EmailMaxChars     = 100_000 // Unicode code points
	PushMaxBytes      = 4_000   // UTF-8 bytes
	PushTokenMaxBytes = 4_096   // UTF-8 bytes
	IdempotencyKeyMax = 255     // characters (data-model.md notifications.idempotency_key)
)

var e164 = regexp.MustCompile(`^\+[1-9]\d{1,14}$`)

// ValidateRecipient returns a message describing why recipient is invalid for ch, or "".
func ValidateRecipient(ch Channel, recipient string) string {
	switch ch {
	case ChannelSMS:
		if !e164.MatchString(recipient) {
			return "must be an E.164 phone number, e.g. +905551234567"
		}
	case ChannelEmail:
		// A bare addr-spec only: no display name, no angle brackets, no surrounding spaces.
		addr, err := mail.ParseAddress(recipient)
		if err != nil || addr.Name != "" || addr.Address != recipient {
			return "must be a valid email address, e.g. user@example.com"
		}
	case ChannelPush:
		token := strings.TrimSpace(recipient)
		if token == "" {
			return "must be a non-empty device token"
		}
		if len(token) > PushTokenMaxBytes {
			return fmt.Sprintf("must be at most %d bytes", PushTokenMaxBytes)
		}
	}
	return ""
}

// ValidateContent returns a message describing why content is invalid for ch, or "". Content is
// checked after template rendering, so the limit applies to what the provider receives.
func ValidateContent(ch Channel, content string) string {
	if strings.TrimSpace(content) == "" {
		return "is required and must not be blank"
	}
	switch ch {
	case ChannelSMS:
		if n := utf8.RuneCountInString(content); n > SMSMaxChars {
			return fmt.Sprintf("must be at most %d characters for sms (got %d)", SMSMaxChars, n)
		}
	case ChannelEmail:
		if n := utf8.RuneCountInString(content); n > EmailMaxChars {
			return fmt.Sprintf("must be at most %d characters for email (got %d)", EmailMaxChars, n)
		}
	case ChannelPush:
		if n := len(content); n > PushMaxBytes {
			return fmt.Sprintf("must be at most %d bytes for push (got %d)", PushMaxBytes, n)
		}
	}
	return ""
}

// CheckContentSource enforces FR-004: exactly one of content or template_id.
func CheckContentSource(content, templateID *string) *ValidationError {
	hasContent := content != nil
	hasTemplate := templateID != nil && *templateID != ""
	switch {
	case hasContent && hasTemplate:
		return NewFieldError("content", "must not be combined with template_id; send one or the other")
	case !hasContent && !hasTemplate:
		return NewFieldError("content", "is required unless template_id is given")
	}
	return nil
}

// ValidateIdempotencyKey checks data-model.md notifications.idempotency_key (1–255 characters).
func ValidateIdempotencyKey(key *string) string {
	if key == nil {
		return ""
	}
	n := utf8.RuneCountInString(*key)
	if n == 0 || strings.TrimSpace(*key) == "" {
		return "must not be empty"
	}
	if n > IdempotencyKeyMax {
		return fmt.Sprintf("must be at most %d characters", IdempotencyKeyMax)
	}
	return ""
}
