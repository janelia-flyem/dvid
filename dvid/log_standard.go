// +build !gcloud

package dvid

import (
	"log"
	"time"
)

type stdLogger struct{}

// Sets up a logger that sends messages via standard log but does not exit or panic.
func newLogger(c interface{}) Logger {
	return stdLogger{}
}

// --- Logger implementation ----

// Debugf formats its arguments analogous to fmt.Printf and records the text as a log
// message at Debug level.  If dvid.Verbose is not true, these logs aren't written.
func (slog stdLogger) Debugf(format string, args ...interface{}) {
	log.Printf("   DEBUG "+format, args)
}

// Infof is like Debugf, but at Info level and will be written regardless if not in
// verbose mode.
func (slog stdLogger) Infof(format string, args ...interface{}) {
	log.Printf("    INFO "+format, args)
}

// Warningf is like Debugf, but at Warning level.
func (slog stdLogger) Warningf(format string, args ...interface{}) {
	log.Printf(" WARNING "+format, args)
}

// Errorf is like Debugf, but at Error level.
func (slog stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("  ERROR "+format, args)
}

// Criticalf is like Debugf, but at Critical level.
func (slog stdLogger) Criticalf(format string, args ...interface{}) {
	log.Printf("CRITICAL "+format, args)
}

// NewTimeLog returns a logger that supports elapsed time from the request.
// Example:
//     mylog := NewTimeLog()
//     ...
//     mylog.Debugf("stuff happened")  // Appends elapsed time from NewTimeLog() to message.
func (slog stdLogger) NewTimeLog() TimeLog {
	return TimeLog{slog, time.Now()}
}
