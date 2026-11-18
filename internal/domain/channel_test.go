package domain

import (
	"errors"
	"testing"
)

func TestParseChannel(t *testing.T) {
	tests := []struct {
		in      string
		want    Channel
		wantErr bool
	}{
		{"sms", ChannelSMS, false},
		{"email", ChannelEmail, false},
		{"push", ChannelPush, false},
		{"SMS", "", true}, // case-sensitive, like the OpenAPI enum
		{"", "", true},
		{"fax", "", true},
	}
	for _, tc := range tests {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseChannel(tc.in)
			if tc.wantErr {
				if !errors.Is(err, ErrInvalidChannel) {
					t.Fatalf("err = %v, want ErrInvalidChannel", err)
				}
				return
			}
			if err != nil || got != tc.want || got.String() != tc.in {
				t.Fatalf("ParseChannel(%q) = %q, %v", tc.in, got, err)
			}
		})
	}
	if len(Channels) != 3 {
		t.Fatalf("Channels = %v", Channels)
	}
}
