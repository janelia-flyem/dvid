// +build !clustered,!gcloud

package dvid

import (
	"fmt"
	"log"

	"gopkg.in/natefinch/lumberjack.v2"
)

type stdLogger struct {
	*lumberjack.Logger
}

var logger stdLogger

type LogConfig struct {
	Logfile string
	MaxSize int `toml:"max_log_size"`
	MaxAge  int `toml:"max_log_age"`
}

// SetLogger creates a logger that saves to a rotating log file.
func SetLogger(c LogConfig) {
	if c.Logfile == "" {
		fmt.Println("Sending log messages to stdout since no log file specified.")
		return
	}
	fmt.Printf("Sending log messages to: %s\n", c.Logfile)
	l := &lumberjack.Logger{
		Filename: c.Logfile,
		MaxSize:  c.MaxSize, // megabytes
		MaxAge:   c.MaxAge,  //days
	}
	log.SetOutput(l)
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
