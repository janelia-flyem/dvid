package dvid

import "sync/atomic"

var denyRequests int32 // default 0 = allow requests

// AllowRequests sets dvid to process requests
func AllowRequests() {
	atomic.StoreInt32(&denyRequests, 0)
}

// DenyRequests sets dvid to ignore requests
func DenyRequests() {
	atomic.StoreInt32(&denyRequests, 1)
}

// RequestsOK returns true if dvid should respond to requests
func RequestsOK() bool {
	return atomic.LoadInt32(&denyRequests) == 0
}
