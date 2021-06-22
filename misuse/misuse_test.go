package misuse

import (
	"context"
	forwarderCommon "github.com/manycore-com/forwarder/common"
	forwarderDb "github.com/manycore-com/forwarder/database"
	forwarderTest "github.com/manycore-com/forwarder/test"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
	"time"
)

func TestAgeOfTs(t *testing.T) {
	now := time.Now().UTC()
	time.Sleep(time.Second * 2)
	assert.Equal(t, int64(2), AgeOfTs(now))
}

func resetTestUser(companyId int) error {
	dbconn, err := forwarderDb.GetDbConnection()
	if nil != err {
		return err
	}

	q := `
    update webhook_forwarder_poll_endpoint set is_active = true where company_id = $1
    `
	_, err = dbconn.Exec(context.Background(), q, companyId)
	if nil != err {
		return err
	}

	q = `
    update webhook_forwarder_poll_cfg set warned_at = null, disabled_at = null where company_id = $1
    `
	_, err = dbconn.Exec(context.Background(), q, companyId)
	if nil != err {
		return err
	}

	return nil
}

func TestProcessMisuseElem(t *testing.T) {
	var warnedAt *time.Time
	var disabledAt *time.Time

	forwarderTest.SetEnvVars()
	forwarderCommon.Env()

	err := resetTestUser(1)
	assert.NoError(t, err)

	os.Setenv("WARNING_SUBJECT", "Warning: the forwarder queue is too big")
	os.Setenv("DISABLE_SUBJECT", "Attention: the forwarder has been disabled due to big queue")
	os.Setenv("EMAIL_SIGNATURE", "Kisses and hugs,\nPlease kill your bugs")

	elem := forwarderDb.CompanyQueueSize{
		CompanyId: 1,
		PresentHour: 13,
		QueueSize: 3000,
		WarnedAt: nil,
		DisabledAt: nil,
		AlertEmail: os.Getenv("FORWARDER_EMAIL_TO"),
	}

	err = ProcessMisuseElem(
		&elem,
		5000,
		10000,
		os.Getenv("WARNING_SUBJECT"),
		os.Getenv("DISABLE_SUBJECT"),
		os.Getenv("EMAIL_SIGNATURE"),
		os.Getenv("SUPPORT_EMAIL"),
		[]string{os.Getenv("SUPPORT_EMAIL_BCC")})

	assert.NoError(t, err)

	forwarderDb.Cleanup()

	ud, err := forwarderDb.GetUserData(1)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.False(t, ud.WarnedAt.Valid)
	assert.False(t, ud.DisabledAt.Valid)



	elem = forwarderDb.CompanyQueueSize{
		CompanyId: 1,
		PresentHour: 13,
		QueueSize: 5500,
		WarnedAt: warnedAt,
		DisabledAt: disabledAt,
		AlertEmail: os.Getenv("FORWARDER_EMAIL_TO"),
	}

	err = ProcessMisuseElem(
		&elem,
		5000,
		10000,
		os.Getenv("WARNING_SUBJECT"),
		os.Getenv("DISABLE_SUBJECT"),
		os.Getenv("EMAIL_SIGNATURE"),
		os.Getenv("SUPPORT_EMAIL"),
		[]string{os.Getenv("SUPPORT_EMAIL_BCC")})

	assert.NoError(t, err)

	forwarderDb.Cleanup()

	ud, err = forwarderDb.GetUserData(1)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.True(t, ud.WarnedAt.Valid)
	assert.False(t, ud.DisabledAt.Valid)

	warnedAt = nil
	disabledAt = nil
	if ud.WarnedAt.Valid {
		warnedAt = &ud.WarnedAt.Time
	}
	if ud.DisabledAt.Valid {
		disabledAt = &ud.DisabledAt.Time
	}



	elem = forwarderDb.CompanyQueueSize{
		CompanyId: 1,
		PresentHour: 13,
		QueueSize: 5500,
		WarnedAt: warnedAt,
		DisabledAt: disabledAt,
		AlertEmail: os.Getenv("FORWARDER_EMAIL_TO"),
	}

	err = ProcessMisuseElem(
		&elem,
		5000,
		10000,
		os.Getenv("WARNING_SUBJECT"),
		os.Getenv("DISABLE_SUBJECT"),
		os.Getenv("EMAIL_SIGNATURE"),
		os.Getenv("SUPPORT_EMAIL"),
		[]string{os.Getenv("SUPPORT_EMAIL_BCC")})

	assert.NoError(t, err)

	forwarderDb.Cleanup()

	ud, err = forwarderDb.GetUserData(1)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.True(t, ud.WarnedAt.Valid)
	assert.False(t, ud.DisabledAt.Valid)


	warnedAt = nil
	disabledAt = nil
	if ud.WarnedAt.Valid {
		warnedAt = &ud.WarnedAt.Time
	}
	if ud.DisabledAt.Valid {
		disabledAt = &ud.DisabledAt.Time
	}



	elem = forwarderDb.CompanyQueueSize{
		CompanyId: 1,
		PresentHour: 13,
		QueueSize: 10500,
		WarnedAt: warnedAt,
		DisabledAt: disabledAt,
		AlertEmail: os.Getenv("FORWARDER_EMAIL_TO"),
	}

	err = ProcessMisuseElem(
		&elem,
		5000,
		10000,
		os.Getenv("WARNING_SUBJECT"),
		os.Getenv("DISABLE_SUBJECT"),
		os.Getenv("EMAIL_SIGNATURE"),
		os.Getenv("SUPPORT_EMAIL"),
		[]string{os.Getenv("SUPPORT_EMAIL_BCC")})

	assert.NoError(t, err)

	forwarderDb.Cleanup()

	ud, err = forwarderDb.GetUserData(1)
	assert.NoError(t, err)
	if err != nil {
		return
	}
	assert.True(t, ud.WarnedAt.Valid)
	assert.True(t, ud.DisabledAt.Valid)


	resetTestUser(1)
}
