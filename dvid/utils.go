package dvid

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
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

var (
	// NumCPU is the number of cores available to this DVID server.
	NumCPU int

	// Mode is a global variable set to the run modes of this DVID process.
	Mode ModeFlag

	// The global, unexported error logger for DVID.
	errorLogger *log.Logger
)

// Bool is a concurrency-safe bool.
type Bool struct {
	mu  sync.RWMutex
	bit bool
}

func (b *Bool) SetTrue() {
	b.mu.Lock()
	b.bit = true
	b.mu.Unlock()
}

func (b *Bool) SetFalse() {
	b.mu.Lock()
	b.bit = false
	b.mu.Unlock()
}

func (b *Bool) Value() bool {
	defer b.mu.RUnlock()
	b.mu.RLock()
	return b.bit
}

// Filename has a base name + extension.
type Filename string

// HasExtensionPrefix returns true if the given string forms a prefix for
// the filename's extension.
func (fname Filename) HasExtensionPrefix(exts ...string) bool {
	for _, ext := range exts {
		parts := strings.Split(string(fname), ".")
		if strings.HasPrefix(parts[len(parts)-1], ext) {
			return true
		}
	}
	return false
}

// DataFromPost returns data submitted in the given key of a POST request.
func DataFromPost(r *http.Request, key string) ([]byte, error) {
	f, _, err := r.FormFile(key)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	return ioutil.ReadAll(f)
}

// Log prints a message via log.Print() depending on the Mode of DVID.
func Log(mode ModeFlag, p ...interface{}) {
	if mode == Normal || mode == Mode {
		if len(p) == 0 {
			log.Println("No message")
		} else {
			log.Printf(p[0].(string), p[1:]...)
		}
	}
}

// Fmt prints a message via fmt.Print() depending on the Mode of DVID.
func Fmt(mode ModeFlag, p ...interface{}) {
	if mode == Normal || mode == Mode {
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
}

// Wait for WaitGroup then print message including time for operation.
// The last arguments are fmt.Printf arguments and should not include the
// newline since one is added in this function.
func WaitToComplete(wg *sync.WaitGroup, mode ModeFlag, startTime time.Time, p ...interface{}) {
	wg.Wait()
	ElapsedTime(mode, startTime, p...)
}

// ElapsedTime prints the time elapsed from the start time with Printf arguments afterwards.
// Example:  ElapsedTime(dvid.Debug, startTime, "Time since launch of %s", funcName)
func ElapsedTime(mode ModeFlag, startTime time.Time, p ...interface{}) {
	var args []interface{}
	if len(p) == 0 {
		args = append(args, "%s\n")
	} else {
		format := p[0].(string) + ": %s\n"
		args = append(args, format)
		args = append(args, p[1:]...)
	}
	args = append(args, time.Since(startTime))
	Log(mode, args...)
}

// WriteJSONFile writes an arbitrary but exportable Go object to a JSON file.
func WriteJSONFile(filename string, value interface{}) error {
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("Failed to create JSON file: %s [%s]", filename, err)
	}
	defer file.Close()
	m, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("Error in writing JSON file: %s [%s]", filename, err)
	}
	var buf bytes.Buffer
	json.Indent(&buf, m, "", "    ")
	buf.WriteTo(file)
	return nil
}

// ReadJSONFile returns a map[string]interface{} with decoded JSON from a file.
// If a file is not organized as a JSON object, an error will be returned.
func ReadJSONFile(filename string) (value map[string]interface{}, err error) {
	var file *os.File
	file, err = os.Open(filename)
	if err != nil {
		return
	}
	defer file.Close()
	var fileBytes []byte
	fileBytes, err = ioutil.ReadAll(file)
	if err != nil {
		return
	}

	var i interface{}
	err = json.Unmarshal(fileBytes, i)
	if err == io.EOF {
		err = fmt.Errorf("No data in JSON file (%s): [%s]", filename, err)
	} else if err != nil {
		err = fmt.Errorf("Error reading JSON file (%s): %s", filename, err)
	} else {
		var ok bool
		value, ok = i.(map[string]interface{})
		if !ok {
			err = fmt.Errorf("JSON file %s top level is not a valid JSON object!", filename)
		}
	}
	return
}
