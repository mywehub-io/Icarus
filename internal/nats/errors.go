package nats

import (
	"errors"
	"strings"
)

// IsTransportError reports whether err indicates the NATS connection is unavailable
// or broken (as opposed to application-level failures such as validation errors).
func IsTransportError(err error) bool {
	needles := []string{
		"connection closed",
		"connection reset",
		"no responders",
		"no suitable peers",
		"disconnected",
		"not connected",
		"nats: timeout",
		"connection refused",
		"broken pipe",
		"eof",
	}
	for err != nil {
		msg := strings.ToLower(err.Error())
		for _, needle := range needles {
			if strings.Contains(msg, needle) {
				return true
			}
		}
		err = errors.Unwrap(err)
	}
	return false
}
