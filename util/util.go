package util

import "runtime"

func GetUserAgentHeader(clientName string) string {
	return clientName + "/" + runtime.Version()
}
