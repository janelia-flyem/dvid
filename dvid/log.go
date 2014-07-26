package dvid

import "time"

type ModeFlag uint

const (
	Normal ModeFlag = iota
	Debug
)

var (
	// Verbose is set when we want to be exceptionally verbose.
	Verbose bool

	// Mode is a global variable set to the run modes of this DVID process.
	Mode ModeFlag
)

// Logger provides a way for the application to log messages at different severities.
// Implementations will vary if the app is in the cloud or on a local server.
type Logger interface {
	// Debugf formats its arguments analogous to fmt.Printf and records the text as a log
	// message at Debug level.  If dvid.Verbose is not true, these logs aren't written.
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

func Debugf(format string, args ...interface{}) {
	logger.Debugf(format, args)
}

func Infof(format string, args ...interface{}) {
	logger.Infof(format, args)
}

func Warningf(format string, args ...interface{}) {
	logger.Warningf(format, args)
}

func Errorf(format string, args ...interface{}) {
	logger.Errorf(format, args)
}

func Criticalf(format string, args ...interface{}) {
	logger.Criticalf(format, args)
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

func NewTimeLog(logger Logger) TimeLog {
	return TimeLog{logger, time.Now()}
}

func (t TimeLog) Debugf(format string, args ...interface{}) {
	t.logger.Debugf(format+": %s\n", append(args, time.Since(t.start)))
}

func (t TimeLog) Infof(format string, args ...interface{}) {
	t.logger.Infof(format+": %s\n", append(args, time.Since(t.start)))
}

func (t TimeLog) Warningf(format string, args ...interface{}) {
	t.logger.Warningf(format+": %s\n", append(args, time.Since(t.start)))
}

func (t TimeLog) Errorf(format string, args ...interface{}) {
	t.logger.Errorf(format+": %s\n", append(args, time.Since(t.start)))
}

func (t TimeLog) Criticalf(format string, args ...interface{}) {
	t.logger.Criticalf(format+": %s\n", append(args, time.Since(t.start)))
}

func (t TimeLog) Shutdown() {
	t.logger.Shutdown()
}
