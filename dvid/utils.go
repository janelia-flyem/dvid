package dvid

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"mime"
	"net/http"
	"os"
	"path/filepath"
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

var (
	// NumCPU is the number of cores available to this DVID server.
	NumCPU int
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

func MaxInt32(a, b int32) int32 {
	if a > b {
		return a
	}
	return b
}

func MinInt32(a, b int32) int32 {
	if a < b {
		return a
	}
	return b
}

// RandomBytes returns a slices of random bytes.
func RandomBytes(numBytes int32) []byte {
	buf := make([]byte, numBytes)
	src := rand.NewSource(time.Now().UnixNano())
	var offset int32
	for {
		val := int64(src.Int63())
		for i := 0; i < 8; i++ {
			if offset >= numBytes {
				return buf
			}
			buf[offset] = byte(val)
			offset++
			val >>= 8
		}
	}
}

// EstimateGoroutines returns the # of goroutines that can be launched
// given a percentage (up to 1.0) of available CPUs (set by command line
// option or # cores) and/or the megabytes (MB) of memory needed for each goroutine.
// A minimum of 1 goroutine is returned.
// TODO: Actually use the required memory provided in argument.  For now,
//  only returns percentage of maximum # of cores.
func EstimateGoroutines(percentCPUs float64, goroutineMB int32) int {
	goroutines := percentCPUs * float64(NumCPU)
	if goroutines < 1.0 {
		return 1
	}
	rounded := int(math.Floor(goroutines + 0.5))
	if rounded > NumCPU {
		return NumCPU
	}
	return rounded
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

// SendHTTP sends data after setting an appropriate Content-Type by examining the
// name and also some byte sniffing.
func SendHTTP(w http.ResponseWriter, r *http.Request, name string, data []byte) {
	// This implementation follows http.serveContent() in the Go standard library.
	sniffLen := 512
	ctypes, haveType := w.Header()["Content-Type"]
	var ctype string
	if !haveType {
		ctype = mime.TypeByExtension(filepath.Ext(name))
		if ctype == "" {
			ctype = http.DetectContentType(data[:sniffLen])
		}
	} else if len(ctypes) > 0 {
		ctype = ctypes[0]
	}
	w.Header().Set("Content-Type", ctype)
	w.WriteHeader(http.StatusOK)
	if r.Method != "HEAD" {
		io.Copy(w, bytes.NewReader(data))
	}
}

// SupportsGzipEncoding returns true if the http requestor can accept gzip encoding.
func SupportsGzipEncoding(r *http.Request) bool {
	for _, v1 := range r.Header["Accept-Encoding"] {
		for _, v2 := range strings.Split(v1, ",") {
			if strings.TrimSpace(v2) == "gzip" {
				return true
			}
		}
	}
	return false
}

// WriteGzip will write already gzip-encoded data to the ResponseWriter unless
// the requestor cannot support it.  In that case, the gzip data is uncompressed
// and sent uncompressed.
func WriteGzip(gzipData []byte, w http.ResponseWriter, r *http.Request) error {
	if SupportsGzipEncoding(r) {
		w.Header().Set("Content-Encoding", "gzip")
		if _, err := w.Write(gzipData); err != nil {
			return err
		}
	} else {
		Debugf("Requestor (%s) not accepting gzip for request (%s), uncompressing %d bytes.\n",
			r.RemoteAddr, r.Method, len(gzipData))
		gzipBuf := bytes.NewBuffer(gzipData)
		gzipReader, err := gzip.NewReader(gzipBuf)
		if err != nil {
			return err
		}
		if _, err = io.Copy(w, gzipReader); err != nil {
			return err
		}
		if err = gzipReader.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Nod to Andrew Gerrand for simple gzip solution:
// See https://groups.google.com/forum/m/?fromgroups#!topic/golang-nuts/eVnTcMwNVjM
type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func MakeGzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		fn(gzipResponseWriter{Writer: gz, ResponseWriter: w}, r)
	}
}
