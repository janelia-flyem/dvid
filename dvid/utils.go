package dvid

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
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
	"reflect"
	"runtime"
	"strings"
	"sync"
	"time"
	"unsafe"
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

// BitsOfInt is the number of bits used for an int
const BitsOfInt = 32 << (^uint(0) >> 63)

func init() {
	// If this is not a little-endian machine, exit because this package is only optimized
	// for these types of machines.
	var check uint32 = 0x01020304
	if *(*byte)(unsafe.Pointer(&check)) != 0x04 {
		fmt.Printf("This machine is not little-endian.  Currently, DVID label compression does not support this machine.\n")
		os.Exit(1)
	}

	if BitsOfInt != 32 && BitsOfInt != 64 {
		fmt.Printf("Unknown architecture with int size of %d bits.  DVID works with 32 or 64 bit architectures.\n", BitsOfInt)
		os.Exit(1)
	}
}

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

// ModInfo gives a user, app and time for a modification
type ModInfo struct {
	User string
	App  string
	Time string
}

// GetModInfo sets and returns a ModInfo using "u" query string.
func GetModInfo(r *http.Request) ModInfo {
	q := r.URL.Query()
	var info ModInfo
	info.User = q.Get("u")
	info.App = q.Get("app")
	info.Time = time.Now().Format(time.RFC3339)
	return info
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

// Converts the given (possibly) relative path into an absolute path,
// relative to the given anchor directory, not the current working directory.
// If the given relativePath is already an absolute path,
// it is returned unchanged.
func ConvertToAbsolute(relativePath string, anchorDir string) (string, error) {
	if filepath.IsAbs(relativePath) {
		return relativePath, nil
	}
	absDir, err := filepath.Abs(anchorDir)
	if err != nil {
		return relativePath, fmt.Errorf("Could not decode TOML config: %v\n", err)
	}
	absPath := filepath.Join(absDir, relativePath)
	return absPath, nil
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

// ReportPanic prints the message and stack trace to stderr, the log, and via email if
// an email notifier was setup.
func ReportPanic(msg, sendingServer string) {
	buf := make([]byte, 1<<16)
	size := runtime.Stack(buf, false)
	stackTrace := "Stack trace from " + sendingServer + ":\n" + string(buf[0:size]) + "\n"

	os.Stderr.WriteString(msg + stackTrace)
	LogImmediately(msg + stackTrace) // bypass log channels.
	if err := SendEmail("DVID panic report", msg+stackTrace, nil, ""); err != nil {
		os.Stderr.WriteString(fmt.Sprintf("Couldn't send email notifcation: %v\n", err))
	}
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

// New8ByteAlignBytes returns a byte slice that has an 8 byte alignment guarantee
// based on the Go compiler spec.  The source []uint64 must be preserved until
// the byte slice is no longer needed.
func New8ByteAlignBytes(numBytes uint32) ([]byte, []uint64) {
	numWords := numBytes / 8
	if numBytes%8 != 0 {
		numWords++
	}
	uint64buf := make([]uint64, numWords)
	bytebuf := AliasUint64ToByte(uint64buf)
	return bytebuf[:numBytes], uint64buf
}

// ByteToUint64 copies a properly aligned byte slice into a []uint64.
func ByteToUint64(b []byte) (out []uint64, err error) {
	if len(b)%8 != 0 {
		return nil, fmt.Errorf("cannot convert byte slice of length %d into []uint64", len(b))
	}
	sz := len(b) / 8
	out = make([]uint64, sz)
	opos := 0
	for i := 0; i < len(b); i += 8 {
		out[opos] = binary.LittleEndian.Uint64(b[i : i+8])
		opos++
	}
	return out, nil
}

// NOTE: The following slice aliasing functions should be used with caution.  The source data
// must be preserved until the alias is released.  This can be done in the following ways:
//  - make sure the source slice is referenced after the alias is no longer needed.
//  - holding it in same structure as the returned slice (as in Block)
//  - use a runtime.KeepAlive(&b) statement to make sure the underlying bytes are not garbage collected.

// AliasByteToUint64 returns a uint64 slice that reuses the passed byte slice.  NOTE: The passed byte slice
// must be aligned for uint64.  Use New8ByteAlignBytes() to allocate for guarantee.
func AliasByteToUint64(b []byte) (out []uint64, err error) {
	if len(b)%uint64Size != 0 || uintptr(unsafe.Pointer(&b[0]))%uint64Size != 0 {
		return nil, fmt.Errorf("bad len, cap, or alignment of dvid.ByteToUint32 len %d", len(b))
	}
	return uint64SliceFromByteSlice(b), nil
}

// AliasByteToUint32 returns a uint32 slice that reuses the passed byte slice.  NOTE: The passed byte slice
// must be aligned for uint32.
func AliasByteToUint32(b []byte) ([]uint32, error) {
	if len(b)%uint32Size != 0 || uintptr(unsafe.Pointer(&b[0]))%uint32Size != 0 {
		return nil, fmt.Errorf("bad len, cap, or alignment of dvid.ByteToUint32 len %d", len(b))
	}
	return uint32SliceFromByteSlice(b), nil
}

// AliasByteToUint16 returns a uint16 slice that reuses the passed byte slice.  NOTE: The passed byte slice
// must be aligned for uint16.
func AliasByteToUint16(b []byte) ([]uint16, error) {
	if len(b)%uint16Size != 0 || uintptr(unsafe.Pointer(&b[0]))%uint16Size != 0 {
		return nil, fmt.Errorf("bad len, cap, or alignment of dvid.ByteToUint16 len %d", len(b))
	}
	return uint16SliceFromByteSlice(b), nil
}

// AliasUint16ToByte returns the underlying byte slice for a uint16 slice.
func AliasUint16ToByte(in []uint16) []byte {
	sh := &reflect.SliceHeader{}
	sh.Len = len(in) * uint16Size
	sh.Cap = len(in) * uint16Size
	sh.Data = (uintptr)(unsafe.Pointer(&in[0]))
	return *(*[]byte)(unsafe.Pointer(sh))
}

// AliasUint32ToByte returns the underlying byte slice for a uint32 slice.
func AliasUint32ToByte(in []uint32) []byte {
	sh := &reflect.SliceHeader{}
	sh.Len = len(in) * uint32Size
	sh.Cap = len(in) * uint32Size
	sh.Data = (uintptr)(unsafe.Pointer(&in[0]))
	return *(*[]byte)(unsafe.Pointer(sh))
}

// AliasUint64ToByte returns the underlying byte slice for a uint64 slice.
func AliasUint64ToByte(in []uint64) []byte {
	sh := &reflect.SliceHeader{}
	sh.Len = len(in) * uint64Size
	sh.Cap = len(in) * uint64Size
	sh.Data = (uintptr)(unsafe.Pointer(&in[0]))
	return *(*[]byte)(unsafe.Pointer(sh))
}

// taken from github.com/alecthomas/unsafeslice
const (
	uint64Size = 8
	uint32Size = 4
	uint16Size = 2
	uint8Size  = 1
)

func newRawSliceHeader(sh *reflect.SliceHeader, b []byte, stride int) *reflect.SliceHeader {
	sh.Len = len(b) / stride
	sh.Cap = len(b) / stride
	sh.Data = (uintptr)(unsafe.Pointer(&b[0]))
	return sh
}

func newSliceHeader(b []byte, stride int) unsafe.Pointer {
	sh := &reflect.SliceHeader{}
	return unsafe.Pointer(newRawSliceHeader(sh, b, stride))
}

func uint64SliceFromByteSlice(b []byte) []uint64 {
	return *(*[]uint64)(newSliceHeader(b, uint64Size))
}

func int64SliceFromByteSlice(b []byte) []int64 {
	return *(*[]int64)(newSliceHeader(b, uint64Size))
}

func uint32SliceFromByteSlice(b []byte) []uint32 {
	return *(*[]uint32)(newSliceHeader(b, uint32Size))
}

func int32SliceFromByteSlice(b []byte) []int32 {
	return *(*[]int32)(newSliceHeader(b, uint32Size))
}

func uint16SliceFromByteSlice(b []byte) []uint16 {
	return *(*[]uint16)(newSliceHeader(b, uint16Size))
}

func int16SliceFromByteSlice(b []byte) []int16 {
	return *(*[]int16)(newSliceHeader(b, uint16Size))
}

func uint8SliceFromByteSlice(b []byte) []uint8 {
	return *(*[]uint8)(newSliceHeader(b, uint8Size))
}

func int8SliceFromByteSlice(b []byte) []int8 {
	return *(*[]int8)(newSliceHeader(b, uint8Size))
}
