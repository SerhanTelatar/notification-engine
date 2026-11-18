package domain

import (
	"strings"
	"testing"
)

func ptr(s string) *string { return &s }

func TestValidateRecipient(t *testing.T) {
	tests := []struct {
		name  string
		ch    Channel
		in    string
		valid bool
	}{
		{"sms e164", ChannelSMS, "+905551234567", true},
		{"sms shortest", ChannelSMS, "+12", true},
		{"sms 15 digits", ChannelSMS, "+123456789012345", true},
		{"sms 16 digits", ChannelSMS, "+1234567890123456", false},
		{"sms no plus", ChannelSMS, "905551234567", false},
		{"sms leading zero", ChannelSMS, "+05551234567", false},
		{"sms spaces", ChannelSMS, "+90 555 123 4567", false},
		{"sms letters", ChannelSMS, "12345", false},
		{"email plain", ChannelEmail, "user@example.com", true},
		{"email plus tag", ChannelEmail, "user+tag@example.co.uk", true},
		{"email display name", ChannelEmail, "Ada <ada@example.com>", false},
		{"email angle brackets", ChannelEmail, "<ada@example.com>", false},
		{"email no at", ChannelEmail, "example.com", false},
		{"email surrounding space", ChannelEmail, " ada@example.com", false},
		{"push token", ChannelPush, "fcm:abc123", true},
		{"push blank", ChannelPush, "   ", false},
		{"push token at limit", ChannelPush, strings.Repeat("a", PushTokenMaxBytes), true},
		{"push token over limit", ChannelPush, strings.Repeat("a", PushTokenMaxBytes+1), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := ValidateRecipient(tc.ch, tc.in)
			if (msg == "") != tc.valid {
				t.Fatalf("ValidateRecipient(%s, %q) = %q, valid=%v", tc.ch, tc.in, msg, tc.valid)
			}
		})
	}
}

func TestValidateContentBoundaries(t *testing.T) {
	// "ğ" is 1 code point but 2 bytes; "😀" is 1 code point but 4 bytes.
	tests := []struct {
		name  string
		ch    Channel
		in    string
		valid bool
	}{
		{"sms 160 ascii", ChannelSMS, strings.Repeat("a", 160), true},
		{"sms 161 ascii", ChannelSMS, strings.Repeat("a", 161), false},
		{"sms 160 multibyte counts as chars", ChannelSMS, strings.Repeat("ğ", 160), true},
		{"sms 161 multibyte", ChannelSMS, strings.Repeat("ğ", 161), false},
		{"email 100000", ChannelEmail, strings.Repeat("a", EmailMaxChars), true},
		{"email 100001", ChannelEmail, strings.Repeat("a", EmailMaxChars+1), false},
		{"push 4000 bytes", ChannelPush, strings.Repeat("a", 4000), true},
		{"push 4001 bytes", ChannelPush, strings.Repeat("a", 4001), false},
		{"push 1000 emoji = 4000 bytes", ChannelPush, strings.Repeat("😀", 1000), true},
		{"push 1001 emoji = 4004 bytes", ChannelPush, strings.Repeat("😀", 1001), false},
		{"blank", ChannelSMS, "  \n\t", false},
		{"empty", ChannelEmail, "", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := ValidateContent(tc.ch, tc.in)
			if (msg == "") != tc.valid {
				t.Fatalf("ValidateContent(%s, len=%d) = %q, valid=%v", tc.ch, len(tc.in), msg, tc.valid)
			}
		})
	}
}

func TestCheckContentSource(t *testing.T) {
	tests := []struct {
		name     string
		content  *string
		template *string
		valid    bool
	}{
		{"content only", ptr("hi"), nil, true},
		{"template only", nil, ptr("0193a3b0-0000-7000-8000-000000000001"), true},
		{"both", ptr("hi"), ptr("0193a3b0-0000-7000-8000-000000000001"), false},
		{"neither", nil, nil, false},
		{"empty template id counts as absent", nil, ptr(""), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			v := CheckContentSource(tc.content, tc.template)
			if (v == nil) != tc.valid {
				t.Fatalf("got %v, valid=%v", v, tc.valid)
			}
			if v != nil && v.Fields[0].Field != "content" {
				t.Fatalf("field = %s, want content", v.Fields[0].Field)
			}
		})
	}
}

func TestValidateIdempotencyKey(t *testing.T) {
	tests := []struct {
		name  string
		in    *string
		valid bool
	}{
		{"absent", nil, true},
		{"normal", ptr("order-42"), true},
		{"255 chars", ptr(strings.Repeat("k", 255)), true},
		{"256 chars", ptr(strings.Repeat("k", 256)), false},
		{"empty", ptr(""), false},
		{"blank", ptr("   "), false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if msg := ValidateIdempotencyKey(tc.in); (msg == "") != tc.valid {
				t.Fatalf("got %q, valid=%v", msg, tc.valid)
			}
		})
	}
}
