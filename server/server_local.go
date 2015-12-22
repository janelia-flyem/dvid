// +build !clustered,!gcloud

/*
	This file supports opening and managing HTTP/RPC servers locally from one process
	instead of using always available services like in a cluster or Google cloud.  It
	also manages local or embedded storage engines.
*/

package server

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"net/smtp"
	"os"
	"runtime"
	"text/template"

	"github.com/janelia-flyem/dvid/datastore"
	"github.com/janelia-flyem/dvid/dvid"
	"github.com/janelia-flyem/go/toml"
)

const (
	// The default URL of the DVID web server
	DefaultWebAddress = "localhost:8000"

	// The default RPC address for command-line use of a remote DVID server
	DefaultRPCAddress = "localhost:8001"

	// The name of the server error log, stored in the datastore directory.
	ErrorLogFilename = "dvid-errors.log"
)

var tc tomlConfig

type tomlConfig struct {
	Server  serverConfig
	Email   emailConfig
	Logging dvid.LogConfig
	Store   dvid.StoreConfig
}

func (c *tomlConfig) HTTPAddress() string {
	return c.Server.HTTPAddress
}

func (c *tomlConfig) RPCAddress() string {
	return c.Server.RPCAddress
}

func (c *tomlConfig) WebClient() string {
	return c.Server.WebClient
}

type serverConfig struct {
	HTTPAddress string
	RPCAddress  string
	WebClient   string

	IIDGen   string `toml:"instance_id_gen"`
	IIDStart uint32 `toml:"instance_id_start"`
}

type emailConfig struct {
	Notify   []string
	Username string
	Password string
	Server   string
	Port     int
}

func (e emailConfig) Host() string {
	return fmt.Sprintf("%s:%d", e.Server, e.Port)
}

func LoadConfig(filename string) (*datastore.InstanceConfig, *dvid.LogConfig, *dvid.StoreConfig, error) {
	if filename == "" {
		return nil, nil, nil, fmt.Errorf("No server TOML configuration file provided")
	}
	if _, err := toml.DecodeFile(filename, &tc); err != nil {
		return nil, nil, nil, fmt.Errorf("Could not decode TOML config: %v\n", err)
	}

	// The server config could be local, cluster, gcloud-specific config.  Here it is local.
	config = &tc
	ic := datastore.InstanceConfig{tc.Server.IIDGen, dvid.InstanceID(tc.Server.IIDStart)}
	return &ic, &(tc.Logging), &(tc.Store), nil
}

type emailData struct {
	From    string
	To      string
	Subject string
	Body    string
	Host    string
}

// Go template
const emailTemplate = `From: {{.From}}
To: {{.To}}
Subject: {{.Subject}}

{{.Body}}

Sincerely,

DVID at {{.Host}}
`

// SendNotification sends e-mail to the given recipients or the default emails loaded
// during configuration.
func SendNotification(message string, recipients []string) error {
	e := tc.Email
	var auth smtp.Auth
	if e.Password != "" {
		auth = smtp.PlainAuth("", e.Username, e.Password, e.Server)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown host"
	}

	for _, recipient := range e.Notify {
		context := &emailData{
			From:    e.Username,
			To:      recipient,
			Subject: "DVID panic report",
			Body:    message,
			Host:    hostname,
		}

		t := template.New("emailTemplate")
		if t, err = t.Parse(emailTemplate); err != nil {
			return fmt.Errorf("error trying to parse mail template: %v", err)
		}

		// Apply the values we have initialized in our struct context to the template.
		var doc bytes.Buffer
		if err = t.Execute(&doc, context); err != nil {
			return fmt.Errorf("error trying to execute mail template: %v", err)
		}

		// Send notification
		err = smtp.SendMail(e.Host(), auth, e.Username, []string{recipient}, doc.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// Serve starts HTTP and RPC servers.  If passed strings are empty, it will use
// the configuration info from the toml.
func Serve() error {
	// Use defaults if not set via TOML config file.
	if tc.Server.HTTPAddress == "" {
		tc.Server.HTTPAddress = DefaultWebAddress
	}
	if tc.Server.RPCAddress == "" {
		tc.Server.RPCAddress = DefaultRPCAddress
	}

	dvid.Infof("------------------\n")
	dvid.Infof("DVID code version: %s\n", gitVersion)
	dvid.Infof("Serving HTTP on %s\n", tc.Server.HTTPAddress)
	dvid.Infof("Serving command-line use via RPC %s\n", tc.Server.RPCAddress)
	dvid.Infof("Using web client files from %s\n", tc.Server.WebClient)
	dvid.Infof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())

	// Launch the web server
	go serveHttp(tc.Server.HTTPAddress, tc.Server.WebClient)

	// Launch the rpc server
	if err := serveRpc(tc.Server.RPCAddress); err != nil {
		return fmt.Errorf("Could not start RPC server: %v\n", err)
	}
	return nil
}

// Listen and serve RPC requests using address.  Should not return if successful.
func serveRpc(address string) error {
	c := new(RPCConnection)
	c.RPCConnection.Address = address
	rpc.Register(c)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	http.Serve(listener, nil)
	return nil
}
