package dvid

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	Kilo = 1 << 10
	Mega = 1 << 20
	Giga = 1 << 30
	Tera = 1 << 40
)

type ModeFlag uint

const (
	Normal ModeFlag = iota
	Debug
	Benchmark
)

// Mode is a global variable set to the run modes of this DVID process.
var Mode ModeFlag

// Log prints a message via log.Print() depending on the Mode of DVID.
func Log(modes ModeFlag, p ...interface{}) {
	if ((modes&Debug) != 0 && Mode == Debug) || ((modes&Benchmark) != 0 && Mode == Benchmark) {
		if len(p) == 0 {
			log.Println("No message")
		} else {
			log.Printf(p[0].(string), p[1:]...)
		}
	}
}

// Fmt prints a message via fmt.Print() depending on the Mode of DVID.
func Fmt(modes ModeFlag, p ...interface{}) {
	if ((modes&Debug) != 0 && Mode == Debug) || ((modes&Benchmark) != 0 && Mode == Benchmark) {
		if len(p) == 0 {
			fmt.Println("No message")
		} else {
			fmt.Printf(p[0].(string), p[1:]...)
		}
	}
}

// Error prints a message to the Error Log File, which is useful to mark potential issues
// but not ones that should crash the DVID server.  Basically, you should opt to crash
// the server if a mistake can propagate and corrupt data.  If not, you can use this function.
// Note that Error logging to a file only occurs if DVID is running as a server, otherwise
// this function will print to stdout.
func Error(p ...interface{}) {
	if len(p) == 0 {
		log.Println("No message")
	} else {
		log.Printf(p[0].(string), p[1:]...)
	}
	if errorLogger != nil {
		if len(p) == 0 {
			errorLogger.Println("No message")
		} else {
			errorLogger.Printf(p[0].(string), p[1:]...)
		}
	}
}

// SetErrorLoggingFile creates an error logger to the given file for this DVID process.
func SetErrorLoggingFile(out io.Writer) {
	errorLogger = log.New(out, "", log.Ldate|log.Ltime|log.Llongfile)
	errorLogger.Println("Starting error logging for DVID")
}

// The global, unexported error logger for DVID.
var errorLogger *log.Logger

// Wait for WaitGroup then print message including time for operation.
// The last arguments are fmt.Printf arguments and should not include the
// newline since one is added in this function.
func WaitToComplete(wg *sync.WaitGroup, startTime time.Time, p ...interface{}) {
	wg.Wait()
	if len(p) == 0 {
		log.Fatalln("Illegal call to WaitToComplete(): No message arguments!")
	}
	format := p[0].(string) + ": %s\n"
	args := p[1:]
	args = append(args, time.Since(startTime))
	fmt.Printf(format, args...)
}

// Prompt returns a string entered by the user after displaying message.
// If the user just hits ENTER (or enters an empty string), then the
// defaultValue is returned.
func Prompt(message, defaultValue string) string {
	fmt.Print(message + " [" + defaultValue + "]: ")
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	line = strings.TrimSpace(line)
	if line == "" {
		return defaultValue
	}
	return line
}
