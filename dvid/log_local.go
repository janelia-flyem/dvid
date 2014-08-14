// +build !clustered,!gcloud

package dvid

import (
	"log"

	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

type stdLogger struct {
	*lumberjack.Logger
}

var logger stdLogger

// Sets up a logger that sends messages via standard log but does not exit or panic.
func NewLogger(c interface{}) Logger {
	filename, ok := c.(string)
	if !ok {
		log.Fatalf("Cannot open logger.  Received %v instead of string\n", c)
		return nil
	}
	l := &lumberjack.Logger{
		Filename: filename,
		MaxSize:  50, // megabytes
	}
	log.SetOutput(l)
	logger = stdLogger{l}
	return logger
}

// --- Logger implementation ----

// Debugf formats its arguments analogous to fmt.Printf and records the text as a log
// message at Debug level.  If dvid.Verbose is not true, these logs aren't written.
func (slog stdLogger) Debugf(format string, args ...interface{}) {
	log.Printf("   DEBUG "+format, args...)
}

// Infof is like Debugf, but at Info level and will be written regardless if not in
// verbose mode.
func (slog stdLogger) Infof(format string, args ...interface{}) {
	log.Printf("    INFO "+format, args...)
}

// Warningf is like Debugf, but at Warning level.
func (slog stdLogger) Warningf(format string, args ...interface{}) {
	log.Printf(" WARNING "+format, args...)
}

// Errorf is like Debugf, but at Error level.
func (slog stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf("  ERROR "+format, args...)
}

// Criticalf is like Debugf, but at Critical level.
func (slog stdLogger) Criticalf(format string, args ...interface{}) {
	log.Printf("CRITICAL "+format, args...)
}

func (slog stdLogger) Shutdown() {
	slog.Rotate()
}
