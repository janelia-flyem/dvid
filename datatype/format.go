package datatype

import (
	"fmt"
	"log"
	"strings"
)

// Format uniquely identifies a DVID-supported data type and how it is arranged
// within a DVID datastore instance.
type Format struct {
	// Name describes a data type & may not be as unique (e.g., "grayscale")
	Name string

	// Url specifies the unique package name that fulfills the DVID Data interface
	Url string

	// IsolateData should be false (default) to place this data type next to
	// other data types within a block, so for a given block we can quickly
	// retrieve a variety of data types across the block's voxels.  If IsolateData
	// is true, we optimize for retrieving this data type independently, e.g., all 
	// the label->label maps across blocks to make a subvolume map on the fly.
	IsolateData bool
}

// Service is an interface that allows arbitrary data types as long as they fulfill
// a few functions required by DVID commands.
type Service interface {
	GetName() string
	GetUrl() string
	GetIsolateData() bool

	// Add provides an implementation of DVID add command specific to a data type
	Add(datastoreDir, filenameGlob, uuidString, params string) (err error)
}

func (format *Format) GetName() string {
	return format.Name
}

func (format *Format) GetUrl() string {
	return format.Url
}

func (format *Format) GetIsolateData() bool {
	return format.IsolateData
}

// Supported is the set of registered data types held as a global variable initialized at runtime.
var Supported map[string]Service

// NamesSupported returns a list of data type names supported by this DVID
func NamesSupported() string {
	var names []string
	for _, s := range Supported {
		names = append(names, s.GetName())
	}
	return strings.Join(names, ", ")
}

// UrlsSupported returns a list of data type urls supported by this DVID
func UrlsSupported() string {
	var urls []string
	for url, _ := range Supported {
		urls = append(urls, url)
	}
	return strings.Join(url, ", ")
}

// RegisterFormat registers a data type for DVID use.
func RegisterFormat(format Service) {
	log.Printf("Registering data type format: %s (%s)", format.GetName(), format.GetUrl())
	if Supported == nil {
		Supported = make(map[string]Service)
	}
	Supported[format.GetUrl()] = format
}

// GetService returns a supported format's service or an error if name is not supported or ambiguous.
func GetService(name string) (service Service, err error) {
	for _, s := range Supported {
		if name == s.GetName() {
			if service != nil {
				err = fmt.Errorf("Given data type (%s) is ambiguous.  Supporting '%s' and '%s'",
					name, service.GetName(), s.GetName())
				service = nil
				return
			} else {
				service = s
			}
		}
	}
	if service == nil {
		err = fmt.Errorf(
			"Data type (%s) is unsupported.  DVID has been compiled with these data types: %s",
			name, NamesSupported())
	}
	return
}
