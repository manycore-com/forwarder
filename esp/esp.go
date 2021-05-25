package esp

func IsOkEsp(esp string) bool {
	if esp == "sg" {
		return true
	}

	return false
}

func SupportedEsp() string {
	return "sg"
}

func GetSignHeaderName(esp string) string {
	if "sg" == esp {
		return "X-Twilio-Email-Event-Webhook-Signature"
	} else {
		return ""
	}
}