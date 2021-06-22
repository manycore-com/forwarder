package misuse

import (
	"fmt"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	"os"
	"time"
)

func Env() error {
	return forwarderCommon.Env()
}

func AgeOfTs(ts time.Time) int64 {
	return time.Since(ts).Nanoseconds() / int64(1000000000)
}

func getWarningMessage(companyId int, queueLevel int, cutOffLevel int, emailSignature string) string {
	return fmt.Sprintf(`Hi,

The queue of messages is alarmingly high: %d
We will disable the forwarder if it goes over %d

Are all your endpoints working ok?

%s
`, queueLevel, cutOffLevel, emailSignature)
}

func getDisableMessage(companyId int, queueLevel int, cutOffLevel int, emailSignature string, supportEmail string) string {
	return fmt.Sprintf(`Hi,

The queue of messages is too high: %d
The maximum allowed level is %d
We've disabled forwarding. 


%s
`, queueLevel, cutOffLevel, emailSignature)
}

func ProcessMisuseElem(elem *forwarderDb.CompanyQueueSize, warningLevel int, cutOffLevel int, warningSubject string, disableSubject string, emailSignature string, supportEmail string, bccList []string) error {
	var err error

	if elem.QueueSize > cutOffLevel && nil == elem.DisabledAt {
		if elem.DisabledAt != nil {
			return nil
		}

		if "" != elem.AlertEmail {
			// Ok, we've come this far. We need to warn user.
			m := getDisableMessage(elem.CompanyId, elem.QueueSize, cutOffLevel, emailSignature, supportEmail)
			err = forwarderCommon.SendMail(
				forwarderCommon.GetDefaultFrom(supportEmail),
				supportEmail,
				[]string{elem.AlertEmail},
				nil,
				bccList,
				disableSubject,
				m)

			if nil != err {
				fmt.Printf("forwarder.misuse.Misuse() Error: Failed to mail warning: %v\n", err)
			}
		}

		err := forwarderDb.DisableCompany(elem.CompanyId)
		if nil != err {
			fmt.Printf("forwarder.misuse.Misuse() Error: Failed to disable company %d: %v\n", elem.CompanyId, err)
		}

	} else if elem.QueueSize > warningLevel {
		if elem.DisabledAt != nil {
			return nil
		}

		if elem.WarnedAt != nil {
			if int64(12 * 3600) > AgeOfTs(*elem.WarnedAt) {
				// User warned less than 12 hours ago.
				return nil
			}
		}

		if "" != elem.AlertEmail {
			// Ok, we've come this far. We need to warn user.
			m := getWarningMessage(elem.CompanyId, elem.QueueSize, cutOffLevel, emailSignature)
			err = forwarderCommon.SendMail(
				forwarderCommon.GetDefaultFrom(supportEmail),
				supportEmail,
				[]string{elem.AlertEmail},
				nil,
				bccList,
				warningSubject,
				m)

			if nil != err {
				fmt.Printf("forwarder.misuse.Misuse() Error: Failed to mail warning: %v\n", err)
			}
		}

		err := forwarderDb.SetWarnedAt(elem.CompanyId)
		if nil != err {
			fmt.Printf("forwarder.misuse.Misuse() Error: Failed to write warned_at to db: %v\n", err)
		}
	}
	return nil
}

func ProcessMisuse(warningLevel int, cutOffLevel int) error {

	warningSubject := os.Getenv("WARNING_SUBJECT")
	if "" == warningSubject {
		warningSubject = "Warning: the forwarder queue is too big"
	}

	disableSubject := os.Getenv("DISABLE_SUBJECT")
	if "" == disableSubject {
		disableSubject = "Attention: the forwarder has been disabled due to big queue"
	}

	emailSignature := os.Getenv("EMAIL_SIGNATURE")
	if "" == emailSignature {
		emailSignature = "Best regards"
	}

	supportEmail := os.Getenv("SUPPORT_EMAIL")
	if "" == supportEmail {
		return fmt.Errorf("environment variable SUPPORT_EMAIL is missing")
	}

	var bccList []string = nil
	supportEmailBcc := os.Getenv("SUPPORT_EMAIL_BCC")
	if "" != supportEmailBcc {
		bccList = []string{supportEmailBcc}
	}

	processUs, err := forwarderDb.GetCompaniesAndQueueSizes()
	if err != nil {
		return fmt.Errorf("failed to get list: %v", err)
	}

	for _, elem := range processUs {
		err = ProcessMisuseElem(elem, warningLevel, cutOffLevel, warningSubject, disableSubject, emailSignature, supportEmail, bccList)
		if nil != err  {
			return err
		}
	}

	return nil
}

