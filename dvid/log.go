package dvid

import (
	"fmt"
	"time"
)

type ModeFlag uint

const (
	DebugMode ModeFlag = iota
	InfoMode
	WarningMode
	ErrorMode
	CriticalMode
	SilentMode
)

var (
	// mode is a global variable set to the run modes of this DVID process.
	mode ModeFlag = InfoMode

	// we use a single goroutine for writing a stream of messages to the log in
	// an asynchronous manner.
	logCh chan logMessage
)

type logFunc func(s string)

type logMessage struct {
	f   logFunc
	msg string
}

const maxPendingLogMessages = 10000

func init() {
	logCh = make(chan logMessage, maxPendingLogMessages)
	go func() {
		for msg := range logCh {
			msg.f(msg.msg)
		}
	}()
}

// PendingLogMessages returns the number of log messages that are in queue to be written.
func PendingLogMessages() int {
	return len(logCh)
}

// Shutdown closes any logging, blocking until the log has been flushed of pending messages.
func Shutdown() {
	logger.Infof("Shutting down DVID core...\n")
	for {
		time.Sleep(1 * time.Second)
		if len(logCh) > 0 {
			Infof("Waiting for %d log messages to write...\n", len(logCh))
		} else {
			break
		}
	}
	close(logCh)
	logger.Infof("Logging system shutdown.\n")
	logger.Shutdown()
}

// Logger provides a way for the application to log messages at different severities.
// Implementations will vary if the app is in the cloud or on a local server.
type Logger interface {
	// Debug writes a string synchronously without formatting using Debug level.
	Debug(s string)

	// Info writes a string synchronously without formatting using Info level.
	Info(s string)

	// Warning writes a string synchronously without formatting using Warning level.
	Warning(s string)

	// Error writes a string synchronously without formatting using Error level.
	Error(s string)

	// Critical writes a string synchronously without formatting using Critical level.
	Critical(s string)

	// Debugf formats its arguments analogous to fmt.Printf and records the text as a log
	// message at Debug level.  If dvid.Verbose is not true, these logs aren't written.
	// Messages are sent through a buffered channel to make logging more asynchronous.
	Debugf(format string, args ...interface{})

	// Infof is like Debugf, but at Info level and will be written regardless if not in
	// verbose mode.
	Infof(format string, args ...interface{})

	// Warningf is like Debugf, but at Warning level.
	Warningf(format string, args ...interface{})

	// Errorf is like Debugf, but at Error level.
	Errorf(format string, args ...interface{})

	// Criticalf is like Debugf, but at Critical level.
	Criticalf(format string, args ...interface{})

	// Shutdown makes sure logs are closed.
	Shutdown()
}

// package print functions use the default package-level logger initialized
// with newLogger() or is simply nil and uses unmodified standard log package.

// SetLogMode sets the severity required for a log message to be printed.
// For example, SetMode(dvid.WarningMode) will log any calls using
// Warningf, Errorf, or Criticalf.  To turn off all logging, use SilentMode.
func SetLogMode(newMode ModeFlag) {
	mode = newMode
}

func Debugf(format string, args ...interface{}) {
	if mode <= DebugMode {
		logCh <- logMessage{f: logger.Debug, msg: fmt.Sprintf(format, args...)}
	}
}

func Infof(format string, args ...interface{}) {
	if mode <= InfoMode {
		logCh <- logMessage{f: logger.Info, msg: fmt.Sprintf(format, args...)}
	}
}

func Warningf(format string, args ...interface{}) {
	if mode <= WarningMode {
		logCh <- logMessage{f: logger.Warning, msg: fmt.Sprintf(format, args...)}
	}
}

func Errorf(format string, args ...interface{}) {
	if mode <= ErrorMode {
		logCh <- logMessage{f: logger.Error, msg: fmt.Sprintf(format, args...)}
	}
}

func Criticalf(format string, args ...interface{}) {
	if mode <= CriticalMode {
		logCh <- logMessage{f: logger.Critical, msg: fmt.Sprintf(format, args...)}
	}
}

func timestr() string {
	t := time.Now()
	return fmt.Sprintf("%d/%02d/%02d %02d:%02d:%02d", t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func TimeDebugf(format string, args ...interface{}) {
	if mode <= DebugMode {
		logCh <- logMessage{f: logger.Debug, msg: fmt.Sprintf(timestr()+" "+format, args...)}
	}
}

func TimeInfof(format string, args ...interface{}) {
	if mode <= InfoMode {
		logCh <- logMessage{f: logger.Info, msg: fmt.Sprintf(timestr()+" "+format, args...)}
	}
}

func TimeWarningf(format string, args ...interface{}) {
	if mode <= WarningMode {
		logCh <- logMessage{f: logger.Warning, msg: fmt.Sprintf(timestr()+" "+format, args...)}
	}
}

func TimeErrorf(format string, args ...interface{}) {
	if mode <= ErrorMode {
		logCh <- logMessage{f: logger.Error, msg: fmt.Sprintf(timestr()+" "+format, args...)}
	}
}

func TimeCriticalf(format string, args ...interface{}) {
	if mode <= CriticalMode {
		logCh <- logMessage{f: logger.Critical, msg: fmt.Sprintf(timestr()+" "+format, args...)}
	}
}

// LogImmediately writes a message to the log file immediately, bypassing any queue of
// log messages.
func LogImmediately(s string) {
	logger.Criticalf("%s", s)
}

// TimeLog adds elapsed time to logging.
// Example:
//     mylog := NewTimeLog()
//     ...
//     mylog.Debugf("stuff happened")  // Appends elapsed time from NewTimeLog() to message.
type TimeLog struct {
	logger Logger
	start  time.Time
}

func NewTimeLog() TimeLog {
	return TimeLog{logger, time.Now()}
}

func (t TimeLog) Debugf(format string, args ...interface{}) {
	if mode <= DebugMode {
		logCh <- logMessage{f: t.logger.Debug, msg: fmt.Sprintf(format+": %s\n", append(args, time.Since(t.start))...)}
	}
}

func (t TimeLog) Infof(format string, args ...interface{}) {
	if mode <= InfoMode {
		logCh <- logMessage{f: t.logger.Info, msg: fmt.Sprintf(format+": %s\n", append(args, time.Since(t.start))...)}
	}
}

func (t TimeLog) Warningf(format string, args ...interface{}) {
	if mode <= WarningMode {
		logCh <- logMessage{f: t.logger.Warning, msg: fmt.Sprintf(format+": %s\n", append(args, time.Since(t.start))...)}
	}
}

func (t TimeLog) Errorf(format string, args ...interface{}) {
	if mode <= ErrorMode {
		logCh <- logMessage{f: t.logger.Error, msg: fmt.Sprintf(format+": %s\n", append(args, time.Since(t.start))...)}
	}
}

func (t TimeLog) Criticalf(format string, args ...interface{}) {
	if mode <= CriticalMode {
		logCh <- logMessage{f: t.logger.Critical, msg: fmt.Sprintf(format+": %s\n", append(args, time.Since(t.start))...)}
	}
}

func (t TimeLog) Shutdown() {
	t.logger.Shutdown()
}
