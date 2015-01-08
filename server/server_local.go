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

	// Maximum number of throttled ops we can handle through API
	MaxThrottledOps = 1
)

var localConfig configT

type configT struct {
	httpAddress  string
	rpcAddress   string
	webClientDir string
	settings     tomlConfig
}

func (c configT) HTTPAddress() string {
	return c.httpAddress
}

func (c configT) RPCAddress() string {
	return c.rpcAddress
}

func (c configT) WebClient() string {
	return c.webClientDir
}

func init() {
	config = &localConfig
}

type tomlConfig struct {
	Server serverConfig
}

type serverConfig struct {
	Notify  []string
	Logging dvid.LogConfig
	Email   smtpServer
}

type smtpServer struct {
	Username string
	Password string
	Server   string
	Port     int
}

func (s smtpServer) Host() string {
	return fmt.Sprintf("%s:%d", s.Server, s.Port)
}

func LoadConfig(filename string) (*dvid.LogConfig, error) {
	if filename == "" {
		return nil, nil
	}
	if _, err := toml.DecodeFile(filename, &(localConfig.settings)); err != nil {
		return nil, fmt.Errorf("Could not decode TOML config: %s\n", err.Error())
	}
	return &(localConfig.settings.Server.Logging), nil
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
	e := localConfig.settings.Server.Email
	var auth smtp.Auth
	if e.Password != "" {
		auth = smtp.PlainAuth("", e.Username, e.Password, e.Server)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown host"
	}

	for _, recipient := range localConfig.settings.Server.Notify {
		context := &emailData{
			From:    e.Username,
			To:      recipient,
			Subject: "DVID panic report",
			Body:    message,
			Host:    hostname,
		}

		t := template.New("emailTemplate")
		if t, err = t.Parse(emailTemplate); err != nil {
			return fmt.Errorf("error trying to parse mail template: %s", err.Error())
		}

		// Apply the values we have initialized in our struct context to the template.
		var doc bytes.Buffer
		if err = t.Execute(&doc, context); err != nil {
			return fmt.Errorf("error trying to execute mail template: %s", err.Error())
		}

		// Send notification
		err = smtp.SendMail(e.Host(), auth, e.Username, []string{recipient}, doc.Bytes())
		if err != nil {
			return err
		}
	}
	return nil
}

// Serve starts HTTP and RPC servers.
func Serve(httpAddress, webClientDir, rpcAddress string) error {
	// Set the package-level config variable
	dvid.Infof("Serving HTTP on %s\n", httpAddress)
	dvid.Infof("Serving command-line use via RPC %s\n", rpcAddress)
	dvid.Infof("Using web client files from %s\n", webClientDir)
	dvid.Infof("Using %d of %d logical CPUs for DVID.\n", dvid.NumCPU, runtime.NumCPU())
	c := config.(*configT)
	c.httpAddress = httpAddress
	c.rpcAddress = rpcAddress
	c.webClientDir = webClientDir

	// Launch the web server
	go serveHttp(httpAddress, webClientDir)

	// Launch the rpc server
	if err := serveRpc(rpcAddress); err != nil {
		return fmt.Errorf("Could not start RPC server: %s\n", err.Error())
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
