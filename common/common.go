package common

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"time"
)

// Remember to set version in goPrivate too.
// https://blog.golang.org/publishing-go-modules#TOC_4.
// git tag v0.9.0
// git push origin v0.9.0

var PackageVersion = "0.9.10"

var smtpHost string
var smtpPort int
var smtpHostUser string
var smtpHostPassword string
var smtpUseTls bool
var defaultFrom string
func Env() error {
	var err error

	smtpHost = os.Getenv("SMTP_HOST")
	if "" == smtpHost {
		return fmt.Errorf("environment variable SMTP_HOST is missing")
	}

	smtpPort, err = strconv.Atoi(os.Getenv("SMTP_PORT"))
	if nil != err {
		return fmt.Errorf("failed to parse environment variable SMTP_PORT as integer: %v", err)
	}

	smtpHostUser = os.Getenv("SMTP_HOST_USER")
	if "" == smtpHostUser {
		return fmt.Errorf("environment variable SMTP_HOST_USER is missing")
	}

	smtpHostPassword = os.Getenv("SMTP_HOST_PASSWORD")
	if "" == smtpHostPassword {
		return fmt.Errorf("environment variable SMTP_HOST_PASSWORD is missing")
	}

	smtpUseTls = false
	x := strings.ToUpper(os.Getenv("SMTP_USE_TLS"))
	if x == "YES" || x == "TRUE" || x == "1" || x == "ON" {
		smtpUseTls = true
	}

	defaultFrom = os.Getenv("EMAIL_DEFAULT_FROM")

	return nil
}

func GetDefaultFrom(alt string) string {
	if "" == defaultFrom {
		return alt
	} else {
		return defaultFrom
	}
}

// SendMail should perhaps be replaced with https://github.com/go-gomail/gomail or similar instead.
func SendMail(from string, replyTo string, to []string, cc []string, bcc []string, subject string, message string) error {

	if "" == from {
		from = defaultFrom
		if "" == from {
			return fmt.Errorf("forwarder.common.SendMail() from is empty, and environment variable EMAIL_DEFAULT_FROM is not set")
		}
	}

	auth := smtp.PlainAuth("", smtpHostUser, smtpHostPassword, smtpHost)

	var client *smtp.Client

	var err error
	if smtpUseTls {
		tlsconfig := &tls.Config {
			InsecureSkipVerify: true,
			//ServerName: smtpHost,  not needed with InsecureSkipVerify
		}

		// Here is the key, you need to call tls.Dial instead of smtp.Dial
		// for smtp servers running on 465 that require an ssl connection
		// from the very beginning (no starttls)
		conn, err := tls.Dial("tcp", fmt.Sprintf("%s:%d", smtpHost, smtpPort), tlsconfig) // max fail
		if err != nil {
			return err
		}

		client, err = smtp.NewClient(conn, smtpHost)
		if err != nil {
			return err
		}

	} else {

		client, err = smtp.Dial(fmt.Sprintf("%s:%d", smtpHost, smtpPort))
		if err != nil {
			return err
		}

	}

	// I looked at smtp.SendMail()
	defer client.Close()

	// Auth
	if err = client.Auth(auth); err != nil {
		return err
	}

	// To && From
	if err = client.Mail(from); err != nil {
		return err
	}

	for _, toAddress := range to {
		if err = client.Rcpt(toAddress); err != nil {
			return err
		}
	}

	wc, err := client.Data()
	if err != nil {
		return err
	}

	var body bytes.Buffer
	body.Write([]byte("Subject: " + subject + "\r\n"))
	if nil != to {
		for _, toAddr := range to {
			body.Write([]byte("To: " + toAddr + "\r\n"))
		}
	}
	if nil != cc {
		for _, ccAddr := range cc {
			body.Write([]byte("Cc: " + ccAddr + "\r\n"))
		}
	}
	if nil != bcc {
		for _, bccAddr := range bcc {
			body.Write([]byte("Bcc: " + bccAddr + "\r\n"))
		}
	}
	body.Write([]byte("From: " + from + "\r\n"))
	if "" != replyTo {
		body.Write([]byte("Reply-To: " + replyTo + "\r\n"))
	}
	body.Write([]byte("Date: " + time.Now().UTC().Format(time.RFC1123Z) + "\r\n"))
	body.Write([]byte("\r\n"))

	body.Write([]byte(message))

	_, err = wc.Write(body.Bytes())
	if err != nil {
		return err
	}

	err = wc.Close()
	if err != nil {
		return err
	}

	return client.Quit()  // client.Close() is deferred
}
