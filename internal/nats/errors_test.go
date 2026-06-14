package nats

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsTransportError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "connection closed", err: errors.New("nats: connection closed"), want: true},
		{name: "wrapped connection closed", err: fmt.Errorf("pull failed: %w", errors.New("consumer info: nats: connection closed")), want: true},
		{name: "no responders", err: errors.New("nats: no responders available for request"), want: true},
		{name: "validation", err: errors.New("stream and consumer names are required"), want: false},
		{name: "timeout pull", err: errors.New("nats: timeout"), want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsTransportError(tt.err); got != tt.want {
				t.Fatalf("IsTransportError() = %v, want %v", got, tt.want)
			}
		})
	}
}
