package common

import (
	"crypto/tls"
	"fmt"
	"gopkg.in/gomail.v2"
	"os"
	"strconv"
	"strings"
)

// Remember to set version in goPrivate too.
// https://blog.golang.org/publishing-go-modules#TOC_4.
// git tag v0.9.0
// git push origin v0.9.0

var PackageVersion = "0.14.3"

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

// SendMail is replaced with a call to go-mail
func SendMail(from string, replyTo string, to []string, cc []string, bcc []string, subject string, message string) error {

	if "" == from {
		from = defaultFrom
		if "" == from {
			return fmt.Errorf("forwarder.common.SendMail() from is empty, and environment variable EMAIL_DEFAULT_FROM is not set")
		}
	}

	m := gomail.NewMessage()
	m.SetHeader("From", from)

	if to != nil {
		m.SetHeader("To", to...)
	}

	if cc != nil {
		m.SetHeader("Cc", cc...)
	}

	if bcc != nil {
		m.SetHeader("Bcc", bcc...)
	}

	m.SetHeader("Subject", subject)

	m.SetBody("text/plain", message)

	d := gomail.NewDialer(smtpHost, smtpPort, smtpHostUser, smtpHostPassword)
	d.TLSConfig = &tls.Config {
		InsecureSkipVerify: true,
		//ServerName: smtpHost,  not needed with InsecureSkipVerify
	}

	return d.DialAndSend(m)
}
