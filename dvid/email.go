package dvid

import (
	"bytes"
	"fmt"
	"net/smtp"
	"os"
	"sync"
	"text/template"
	"time"
)

// global email config
var emailCfg EmailConfig

type sentTimes struct {
	topics map[string]time.Time
	sync.Mutex
}

var lastSent sentTimes

func init() {
	lastSent.topics = make(map[string]time.Time)
}

// EmailConfig holds information necessary for SMTP use.
type EmailConfig struct {
	Notify   []string
	Username string
	Password string
	Server   string
	Port     int
}

// IsAvailable returns true if the server and port are set to non-zero values.
func (e EmailConfig) IsAvailable() bool {
	if e.Server != "" && e.Port != 0 {
		return true
	}
	return false
}

// Host returns the email server information.
func (e EmailConfig) Host() string {
	return fmt.Sprintf("%s:%d", e.Server, e.Port)
}

type emailData struct {
	From    string
	To      string
	Subject string
	Body    string
	Host    string
}

// SetEmailServer sets the email server used for all subsequent SendEmail calls.
func SetEmailServer(e EmailConfig) {
	emailCfg = e
}

// SendEmail sends e-mail to the given recipients or the default emails loaded
// during configuration.  If a "periodicTopic" is set, then only one email per
// ten minutes is sent for that particular topic.
func SendEmail(subject, message string, recipients []string, periodicTopic string) error {
	if !emailCfg.IsAvailable() {
		return nil
	}
	return emailCfg.sendEmail(subject, message, recipients, periodicTopic)
}

// Go template
const emailTemplate = `From: {{.From}}
To: {{.To}}
Subject: {{.Subject}}

{{.Body}}

Sincerely,

DVID at {{.Host}}
`

func (e EmailConfig) sendEmail(subject, message string, recipients []string, periodicTopic string) error {
	if len(e.Notify) == 0 && len(recipients) == 0 {
		return nil
	}
	if len(recipients) == 0 {
		recipients = e.Notify
	}

	if periodicTopic != "" {
		lastSent.Lock()
		last := lastSent.topics[periodicTopic]
		now := time.Now()
		diff := now.Sub(last)
		if diff.Minutes() < 10.0 {
			lastSent.Unlock()
			return nil
		}
		lastSent.topics[periodicTopic] = now
		lastSent.Unlock()
	}

	var auth smtp.Auth
	if e.Password != "" {
		auth = smtp.PlainAuth("", e.Username, e.Password, e.Server)
	}
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "Unknown host"
	}

	if subject == "" {
		subject = "DVID report"
	}
	for _, recipient := range recipients {
		context := &emailData{
			From:    e.Username,
			To:      recipient,
			Subject: subject,
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
