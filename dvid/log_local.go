// +build !clustered,!gcloud

package dvid

import (
	"fmt"
	"log"

	"github.com/natefinch/lumberjack"
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
func (c *LogConfig) SetLogger() {
	if c == nil || c.Logfile == "" {
		Infof("Sending log messages to stdout since no log file specified.")
		return
	}
	fmt.Printf("Sending log messages to: %s\n", c.Logfile)
	l := &lumberjack.Logger{
		Filename: c.Logfile,
		MaxSize:  c.MaxSize, // megabytes
		MaxAge:   c.MaxAge,  //days
	}
	log.SetOutput(l)
	logger = stdLogger{l}
}

// --- Logger implementation ----

// Debug writes directly to logger at DEBUG level.
func (slog stdLogger) Debug(s string) {
	if logger.Logger != nil {
		logger.Write([]byte(" DEBUG " + s))
	} else {
		log.Printf("%s", " DEBUG "+s)
	}
}

// Info writes directly to logger at INFO level
func (slog stdLogger) Info(s string) {
	if logger.Logger != nil {
		logger.Write([]byte(" INFO " + s))
	} else {
		log.Printf("%s", " INFO "+s)
	}
}

// Warning writes directly to logger at INFO level
func (slog stdLogger) Warning(s string) {
	if logger.Logger != nil {
		logger.Write([]byte(" WARNING " + s))
	} else {
		log.Printf("%s", " WARNING "+s)
	}
}

// Error writes directly to logger at ERROR level
func (slog stdLogger) Error(s string) {
	if logger.Logger != nil {
		logger.Write([]byte(" ERROR " + s))
	} else {
		log.Printf("%s", " ERROR "+s)
	}
}

// Critical writes directly to logger at CRITICAL level
func (slog stdLogger) Critical(s string) {
	if logger.Logger != nil {
		logger.Write([]byte(" CRITICAL " + s))
	} else {
		log.Printf("%s", " CRITICAL "+s)
	}
}

// Debugf formats its arguments analogous to fmt.Printf and records the text as a log
// message at Debug level.
func (slog stdLogger) Debugf(format string, args ...interface{}) {
	log.Printf(" DEBUG "+format, args...)
}

// Infof is like Debugf, but at Info level and will be written regardless if not in
// verbose mode.
func (slog stdLogger) Infof(format string, args ...interface{}) {
	log.Printf(" INFO "+format, args...)
}

// Warningf is like Debugf, but at Warning level.
func (slog stdLogger) Warningf(format string, args ...interface{}) {
	log.Printf(" WARNING "+format, args...)
}

// Errorf is like Debugf, but at Error level.
func (slog stdLogger) Errorf(format string, args ...interface{}) {
	log.Printf(" ERROR "+format, args...)
}

// Criticalf is like Debugf, but at Critical level.
func (slog stdLogger) Criticalf(format string, args ...interface{}) {
	log.Printf(" CRITICAL "+format, args...)
}

func (slog stdLogger) Shutdown() {
	log.Printf("Closing log file...\n")
	if slog.Logger != nil {
		slog.Close()
	}
}
