// +build gcloud

package dvid

import (
	"time"

	"appengine"
)

type gcloudLogger struct {
	appengine.Context
}

// Sets up a logger that sends messages via standard log but does not exit or panic.
func newLogger(ctx interface{}) Logger {
	c := ctx.(appengine.Context)
	return gcloudLogger{c}
}

// --- Logger implementation ----

// Debugf formats its arguments analogous to fmt.Printf and records the text as a log
// message at Debug level.  If dvid.Verbose is not true, these logs aren't written.
func (glog gcloudLogger) Debugf(format string, args ...interface{}) {
	glog.Debugf(format, args)
}

// Infof is like Debugf, but at Info level and will be written regardless if not in
// verbose mode.
func (glog gcloudLogger) Infof(format string, args ...interface{}) {
	glog.Infof(format, args)
}

// Warningf is like Debugf, but at Warning level.
func (glog gcloudLogger) Warningf(format string, args ...interface{}) {
	glog.Warningf(format, args)
}

// Errorf is like Debugf, but at Error level.
func (glog gcloudLogger) Errorf(format string, args ...interface{}) {
	glog.Errorf(format, args)
}

// Criticalf is like Debugf, but at Critical level.
func (glog gcloudLogger) Criticalf(format string, args ...interface{}) {
	glog.Criticalf(format, args)
}

// NewTimeLog returns a logger that supports elapsed time from the request.
// Example:
//     mylog := NewTimeLog()
//     ...
//     mylog.Debugf("stuff happened")  // Appends elapsed time from NewTimeLog() to message.
func (glog gcloudLogger) NewTimeLog() TimeLog {
	return TimeLog{glog, time.Now()}
}
