package forwarderDb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	forwarderStats "github.com/manycore-com/forwarder/stats"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var dbUsageMutex sync.Mutex

var globalDb *pgx.Conn = nil
func GetDbConnection() (*pgx.Conn, error) {

	if nil != globalDb {
		return globalDb, nil
	}

	var (
		dbUser                 = os.Getenv("DB_USER")                  // e.g. 'my-db-user'
		dbPwd                  = os.Getenv("DB_PASS")                  // e.g. 'my-db-password'
		instanceConnectionName = os.Getenv("INSTANCE_CONNECTION_NAME") // e.g. 'project:region:instance'
		dbName                 = os.Getenv("DB_NAME")                  // e.g. 'my-database'
	)

	var dbURI string
	// local or not?
	if strings.Contains(instanceConnectionName, ":") {
		// prod
		socketDir, isSet := os.LookupEnv("DB_SOCKET_DIR")
		if !isSet {
			socketDir = "/cloudsql"
		}

		dbURI = fmt.Sprintf("user=%s password=%s database=%s host=%s/%s", dbUser, dbPwd, dbName, socketDir, instanceConnectionName)
	} else {
		dbURI = fmt.Sprintf("user=%s password=%s database=%s host=%s", dbUser, dbPwd, dbName, instanceConnectionName)
	}

	conn, err := pgx.Connect(context.Background(), dbURI)

	if err != nil {
		return nil, err
	}

	globalDb = conn
	return conn, nil
}

type CompanyInfo struct {
	ForwardUrl    []string
	Secret        string
}

type OneJsonRow struct {
	Dest          string `json:"dest"`
}

var companyInfoMap = make(map[int]*CompanyInfo)

func CheckDb() error {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return err
	}

	var minid int
	q := `select min(id) as x from webhook_forwarder_poll_endpoint`
	err = dbconn.QueryRow(context.Background(), q).Scan(&minid)
	if nil != err {
		return err
	}
	return nil
}

func GetUserData(companyId int) (*CompanyInfo, error) {

	// FIXME not necessarily ideal. A db read blocks a cached read.
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	theElem, elementExists := companyInfoMap[companyId]
	if ! elementExists {
		dbconn, err := GetDbConnection()
		if nil != err {
			return nil, err
		}

		var jsonStr string
		var secret string
		q := `
        select 
            coalesce((
                select array_to_json(array_agg(row_to_json(t)))
                from (
                    select 
                        forward_endpoint as dest
                    from
                        webhook_forwarder_poll_endpoint ipe
                    where
                        ipe.company_id = ipc.company_id AND
                        ipe.is_active = true
                ) t
            ),'[]') as json,
            secret
        from 
            webhook_forwarder_poll_cfg ipc
        where company_id = $1
        `

		err = dbconn.QueryRow(context.Background(), q, companyId).Scan(&jsonStr, &secret)
		if err != nil {
			if strings.Contains(fmt.Sprintf("%v", err), "no rows in result set") {
				companyInfoMap[companyId] = nil
				return nil, nil
			}

			companyInfoMap[companyId] = nil
			return nil, err
		}

		var ci = CompanyInfo{Secret: secret}
		companyInfoMap[companyId] = &ci
		theElem = &ci

		var jsonRows []OneJsonRow
		if "" != jsonStr && "[]" != jsonStr {
			err = json.Unmarshal([]byte(jsonStr), &jsonRows)
			if err != nil {
				companyInfoMap[companyId] = nil
				return nil, err
			}

			for _, s := range jsonRows {
				ci.ForwardUrl = append(ci.ForwardUrl, s.Dest)
			}
		}
	}

	return theElem, nil
}



func UpdateUsage(companyId int, incQueue1 int, incQueue2 int, incQueue3 int, lost int, forwarded int, errorMessage string, totalIncomingMessagesDelta int) (int, int, int, error) {

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return -1, -1, -1, err
	}

	t := time.Now().UTC()
	hourNow := t.Hour()
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	hour := t.Hour()
	q1 := [24]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	q2 := [24]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	q3 := [24]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	l := [24]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	s := [24]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}

	q1[hour] = incQueue1
	q2[hour] = incQueue2
	q3[hour] = incQueue3
	l[hour] = lost
	s[hour] = forwarded

	var lastErrors []string
	var lastHourWithExamples int
	var id int

	// First we try a normal update, and if that fails we do an upsert.
	q := `
UPDATE webhook_forwarder_daily_forward_stats as o
SET 
    q1_h00 = o.q1_h00 + $1, q1_h01 = o.q1_h01 + $2, q1_h02 = o.q1_h02 + $3, q1_h03 = o.q1_h03 + $4,
    q1_h04 = o.q1_h04 + $5, q1_h05 = o.q1_h05 + $6, q1_h06 = o.q1_h06 + $7, q1_h07 = o.q1_h07 + $8,
    q1_h08 = o.q1_h08 + $9, q1_h09 = o.q1_h09 + $10, q1_h10 = o.q1_h10 + $11, q1_h11 = o.q1_h11 + $12,
    q1_h12 = o.q1_h12 + $13, q1_h13 = o.q1_h13 + $14, q1_h14 = o.q1_h14 + $15, q1_h15 = o.q1_h15 + $16,
    q1_h16 = o.q1_h16 + $17, q1_h17 = o.q1_h17 + $18, q1_h18 = o.q1_h18 + $19, q1_h19 = o.q1_h19 + $20,
    q1_h20 = o.q1_h20 + $21, q1_h21 = o.q1_h21 + $22, q1_h22 = o.q1_h22 + $23, q1_h23 = o.q1_h23 + $24, 

    q2_h00 = o.q2_h00 + $25, q2_h01 = o.q2_h01 + $26, q2_h02 = o.q2_h02 + $27, q2_h03 = o.q2_h03 + $28,
    q2_h04 = o.q2_h04 + $29, q2_h05 = o.q2_h05 + $30, q2_h06 = o.q2_h06 + $31, q2_h07 = o.q2_h07 + $32,
    q2_h08 = o.q2_h08 + $33, q2_h09 = o.q2_h09 + $34, q2_h10 = o.q2_h10 + $35, q2_h11 = o.q2_h11 + $36,
    q2_h12 = o.q2_h12 + $37, q2_h13 = o.q2_h13 + $38, q2_h14 = o.q2_h14 + $39, q2_h15 = o.q2_h15 + $40,
    q2_h16 = o.q2_h16 + $41, q2_h17 = o.q2_h17 + $42, q2_h18 = o.q2_h18 + $43, q2_h19 = o.q2_h19 + $44,
    q2_h20 = o.q2_h20 + $45, q2_h21 = o.q2_h21 + $46, q2_h22 = o.q2_h22 + $47, q2_h23 = o.q2_h23 + $48, 

    q3_h00 = o.q3_h00 + $49, q3_h01 = o.q3_h01 + $50, q3_h02 = o.q3_h02 + $51, q3_h03 = o.q3_h03 + $52,
    q3_h04 = o.q3_h04 + $53, q3_h05 = o.q3_h05 + $54, q3_h06 = o.q3_h06 + $55, q3_h07 = o.q3_h07 + $56,
    q3_h08 = o.q3_h08 + $57, q3_h09 = o.q3_h09 + $58, q3_h10 = o.q3_h10 + $59, q3_h11 = o.q3_h11 + $60,
    q3_h12 = o.q3_h12 + $61, q3_h13 = o.q3_h13 + $62, q3_h14 = o.q3_h14 + $63, q3_h15 = o.q3_h15 + $64,
    q3_h16 = o.q3_h16 + $65, q3_h17 = o.q3_h17 + $66, q3_h18 = o.q3_h18 + $67, q3_h19 = o.q3_h19 + $68,
    q3_h20 = o.q3_h20 + $69, q3_h21 = o.q3_h21 + $70, q3_h22 = o.q3_h22 + $71, q3_h23 = o.q3_h23 + $72, 

    l_h00 = o.l_h00 + $73, l_h01 = o.l_h01 + $74, l_h02 = o.l_h02 + $75, l_h03 = o.l_h03 + $76,
    l_h04 = o.l_h04 + $77, l_h05 = o.l_h05 + $78, l_h06 = o.l_h06 + $79, l_h07 = o.l_h07 + $80,
    l_h08 = o.l_h08 + $81, l_h09 = o.l_h09 + $82, l_h10 = o.l_h10 + $83, l_h11 = o.l_h11 + $84,
    l_h12 = o.l_h12 + $85, l_h13 = o.l_h13 + $86, l_h14 = o.l_h14 + $87, l_h15 = o.l_h15 + $88,
    l_h16 = o.l_h16 + $89, l_h17 = o.l_h17 + $90, l_h18 = o.l_h18 + $91, l_h19 = o.l_h19 + $92,
    l_h20 = o.l_h20 + $93, l_h21 = o.l_h21 + $94, l_h22 = o.l_h22 + $95, l_h23 = o.l_h23 + $96, 

    s_h00 = o.s_h00 + $97, s_h01 = o.s_h01 + $98, s_h02 = o.s_h02 + $99, s_h03 = o.s_h03 + $100,
    s_h04 = o.s_h04 + $101, s_h05 = o.s_h05 + $102, s_h06 = o.s_h06 + $103, s_h07 = o.s_h07 + $104,
    s_h08 = o.s_h08 + $105, s_h09 = o.s_h09 + $106, s_h10 = o.s_h10 + $107, s_h11 = o.s_h11 + $108,
    s_h12 = o.s_h12 + $109, s_h13 = o.s_h13 + $110, s_h14 = o.s_h14 + $111, s_h15 = o.s_h15 + $112,
    s_h16 = o.s_h16 + $113, s_h17 = o.s_h17 + $114, s_h18 = o.s_h18 + $115, s_h19 = o.s_h19 + $116,
    s_h20 = o.s_h20 + $117, s_h21 = o.s_h21 + $118, s_h22 = o.s_h22 + $119, s_h23 = o.s_h23 + $120, 

    total_incoming_messages = o.total_incoming_messages + $121
where
    company_id = $122 AND
    created_at = $123
returning last_errors, last_hour_with_examples, id
`
	errUpdate := dbconn.QueryRow(context.Background(), q,
		q1[0], q1[1], q1[2], q1[3], q1[4], q1[5], q1[6], q1[7], q1[8], q1[9], q1[10], q1[11],
		q1[12], q1[13], q1[14], q1[15], q1[16], q1[17], q1[18], q1[19], q1[20], q1[21], q1[22], q1[23],
		q2[0], q2[1], q2[2], q2[3], q2[4], q2[5], q2[6], q2[7], q2[8], q2[9], q2[10], q2[11],
		q2[12], q2[13], q2[14], q2[15], q2[16], q2[17], q2[18], q2[19], q2[20], q2[21], q2[22], q2[23],
		q3[0], q3[1], q3[2], q3[3], q3[4], q3[5], q3[6], q3[7], q3[8], q3[9], q3[10], q3[11],
		q3[12], q3[13], q3[14], q3[15], q3[16], q3[17], q3[18], q3[19], q3[20], q3[21], q3[22], q3[23],
		l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11],
		l[12], l[13], l[14], l[15], l[16], l[17], l[18], l[19], l[20], l[21], l[22], l[23],
		s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], s[10], s[11],
		s[12], s[13], s[14], s[15], s[16], s[17], s[18], s[19], s[20], s[21], s[22], s[23],
		totalIncomingMessagesDelta,
		companyId, day).Scan(&lastErrors, &lastHourWithExamples, &id)

	if nil != errUpdate {
		// s = successfully received into Fanout
		// q1 = successful fanout writes
		// q2 = first forward attempt failed, and msg is put on q2 without errors
		// q3 = second forward attempt failed, and msg is put on q3 without errors
		// l = lost: too many retries / unrecoverable error
		q = `
INSERT INTO webhook_forwarder_daily_forward_stats as o (
    company_id,
    created_at, 
    q1_h00, q1_h01, q1_h02, q1_h03, q1_h04, q1_h05, q1_h06, q1_h07, q1_h08, q1_h09, q1_h10, q1_h11,
    q1_h12, q1_h13, q1_h14, q1_h15, q1_h16, q1_h17, q1_h18, q1_h19, q1_h20, q1_h21, q1_h22, q1_h23,
    q2_h00, q2_h01, q2_h02, q2_h03, q2_h04, q2_h05, q2_h06, q2_h07, q2_h08, q2_h09, q2_h10, q2_h11,
    q2_h12, q2_h13, q2_h14, q2_h15, q2_h16, q2_h17, q2_h18, q2_h19, q2_h20, q2_h21, q2_h22, q2_h23,
    q3_h00, q3_h01, q3_h02, q3_h03, q3_h04, q3_h05, q3_h06, q3_h07, q3_h08, q3_h09, q3_h10, q3_h11,
    q3_h12, q3_h13, q3_h14, q3_h15, q3_h16, q3_h17, q3_h18, q3_h19, q3_h20, q3_h21, q3_h22, q3_h23,
    l_h00, l_h01, l_h02, l_h03, l_h04, l_h05, l_h06, l_h07, l_h08, l_h09, l_h10, l_h11,
    l_h12, l_h13, l_h14, l_h15, l_h16, l_h17, l_h18, l_h19, l_h20, l_h21, l_h22, l_h23,
    s_h00, s_h01, s_h02, s_h03, s_h04, s_h05, s_h06, s_h07, s_h08, s_h09, s_h10, s_h11,
    s_h12, s_h13, s_h14, s_h15, s_h16, s_h17, s_h18, s_h19, s_h20, s_h21, s_h22, s_h23,
    last_errors,
    last_hour_with_examples,
    total_incoming_messages
) values (
    $1,
    $2,
    $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
    $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
    $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74,
    $75, $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92, $93, $94, $95, $96, $97, $98,
    $99, $100, $101, $102, $103, $104, $105, $106, $107, $108, $109, $110, $111, $112, $113, $114, $115, $116, $117, $118, $119, $120, $121, $122,
    ARRAY[]::varchar[],
    -1,
    $123
)
ON CONFLICT (company_id, created_at) DO UPDATE 
SET 
    q1_h00 = o.q1_h00 + $124, q1_h01 = o.q1_h01 + $125, q1_h02 = o.q1_h02 + $126, q1_h03 = o.q1_h03 + $127,
    q1_h04 = o.q1_h04 + $128, q1_h05 = o.q1_h05 + $129, q1_h06 = o.q1_h06 + $130, q1_h07 = o.q1_h07 + $131,
    q1_h08 = o.q1_h08 + $132, q1_h09 = o.q1_h09 + $133, q1_h10 = o.q1_h10 + $134, q1_h11 = o.q1_h11 + $135,
    q1_h12 = o.q1_h12 + $136, q1_h13 = o.q1_h13 + $137, q1_h14 = o.q1_h14 + $138, q1_h15 = o.q1_h15 + $139,
    q1_h16 = o.q1_h16 + $140, q1_h17 = o.q1_h17 + $141, q1_h18 = o.q1_h18 + $142, q1_h19 = o.q1_h19 + $143,
    q1_h20 = o.q1_h20 + $144, q1_h21 = o.q1_h21 + $145, q1_h22 = o.q1_h22 + $146, q1_h23 = o.q1_h23 + $147, 

    q2_h00 = o.q2_h00 + $148, q2_h01 = o.q2_h01 + $149, q2_h02 = o.q2_h02 + $150, q2_h03 = o.q2_h03 + $151,
    q2_h04 = o.q2_h04 + $152, q2_h05 = o.q2_h05 + $153, q2_h06 = o.q2_h06 + $154, q2_h07 = o.q2_h07 + $155,
    q2_h08 = o.q2_h08 + $156, q2_h09 = o.q2_h09 + $157, q2_h10 = o.q2_h10 + $158, q2_h11 = o.q2_h11 + $159,
    q2_h12 = o.q2_h12 + $160, q2_h13 = o.q2_h13 + $161, q2_h14 = o.q2_h14 + $162, q2_h15 = o.q2_h15 + $163,
    q2_h16 = o.q2_h16 + $164, q2_h17 = o.q2_h17 + $165, q2_h18 = o.q2_h18 + $166, q2_h19 = o.q2_h19 + $167,
    q2_h20 = o.q2_h20 + $168, q2_h21 = o.q2_h21 + $169, q2_h22 = o.q2_h22 + $170, q2_h23 = o.q2_h23 + $171, 

    q3_h00 = o.q3_h00 + $172, q3_h01 = o.q3_h01 + $173, q3_h02 = o.q3_h02 + $174, q3_h03 = o.q3_h03 + $175,
    q3_h04 = o.q3_h04 + $176, q3_h05 = o.q3_h05 + $177, q3_h06 = o.q3_h06 + $178, q3_h07 = o.q3_h07 + $179,
    q3_h08 = o.q3_h08 + $180, q3_h09 = o.q3_h09 + $181, q3_h10 = o.q3_h10 + $182, q3_h11 = o.q3_h11 + $183,
    q3_h12 = o.q3_h12 + $184, q3_h13 = o.q3_h13 + $185, q3_h14 = o.q3_h14 + $186, q3_h15 = o.q3_h15 + $187,
    q3_h16 = o.q3_h16 + $188, q3_h17 = o.q3_h17 + $189, q3_h18 = o.q3_h18 + $190, q3_h19 = o.q3_h19 + $191,
    q3_h20 = o.q3_h20 + $192, q3_h21 = o.q3_h21 + $193, q3_h22 = o.q3_h22 + $194, q3_h23 = o.q3_h23 + $195, 

    l_h00 = o.l_h00 + $196, l_h01 = o.l_h01 + $197, l_h02 = o.l_h02 + $198, l_h03 = o.l_h03 + $199,
    l_h04 = o.l_h04 + $200, l_h05 = o.l_h05 + $201, l_h06 = o.l_h06 + $202, l_h07 = o.l_h07 + $203,
    l_h08 = o.l_h08 + $204, l_h09 = o.l_h09 + $205, l_h10 = o.l_h10 + $206, l_h11 = o.l_h11 + $207,
    l_h12 = o.l_h12 + $208, l_h13 = o.l_h13 + $209, l_h14 = o.l_h14 + $210, l_h15 = o.l_h15 + $211,
    l_h16 = o.l_h16 + $212, l_h17 = o.l_h17 + $213, l_h18 = o.l_h18 + $214, l_h19 = o.l_h19 + $215,
    l_h20 = o.l_h20 + $216, l_h21 = o.l_h21 + $217, l_h22 = o.l_h22 + $218, l_h23 = o.l_h23 + $219, 

    s_h00 = o.s_h00 + $220, s_h01 = o.s_h01 + $221, s_h02 = o.s_h02 + $222, s_h03 = o.s_h03 + $223,
    s_h04 = o.s_h04 + $224, s_h05 = o.s_h05 + $225, s_h06 = o.s_h06 + $226, s_h07 = o.s_h07 + $227,
    s_h08 = o.s_h08 + $228, s_h09 = o.s_h09 + $229, s_h10 = o.s_h10 + $230, s_h11 = o.s_h11 + $231,
    s_h12 = o.s_h12 + $232, s_h13 = o.s_h13 + $233, s_h14 = o.s_h14 + $234, s_h15 = o.s_h15 + $235,
    s_h16 = o.s_h16 + $236, s_h17 = o.s_h17 + $237, s_h18 = o.s_h18 + $238, s_h19 = o.s_h19 + $239,
    s_h20 = o.s_h20 + $240, s_h21 = o.s_h21 + $241, s_h22 = o.s_h22 + $242, s_h23 = o.s_h23 + $243,

    total_incoming_messages = o.total_incoming_messages + $244

returning last_errors, last_hour_with_examples, id
`
		err = dbconn.QueryRow(context.Background(), q, companyId, day,
			q1[0],  q1[1],  q1[2],  q1[3],  q1[4],  q1[5],  q1[6],  q1[7],  q1[8],  q1[9],  q1[10], q1[11],
			q1[12], q1[13], q1[14], q1[15], q1[16], q1[17], q1[18], q1[19], q1[20], q1[21], q1[22], q1[23],
			q2[0],  q2[1],  q2[2],  q2[3],  q2[4],  q2[5],  q2[6],  q2[7],  q2[8],  q2[9],  q2[10], q2[11],
			q2[12], q2[13], q2[14], q2[15], q2[16], q2[17], q2[18], q2[19], q2[20], q2[21], q2[22], q2[23],
			q3[0],  q3[1],  q3[2],  q3[3],  q3[4],  q3[5],  q3[6],  q3[7],  q3[8],  q3[9],  q3[10], q3[11],
			q3[12], q3[13], q3[14], q3[15], q3[16], q3[17], q3[18], q3[19], q3[20], q3[21], q3[22], q3[23],
			l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11],
			l[12], l[13], l[14], l[15], l[16], l[17], l[18], l[19], l[20], l[21], l[22], l[23],
			s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], s[10], s[11],
			s[12], s[13], s[14], s[15], s[16], s[17], s[18], s[19], s[20], s[21], s[22], s[23],
			totalIncomingMessagesDelta,
			q1[0], q1[1], q1[2], q1[3], q1[4], q1[5], q1[6], q1[7], q1[8], q1[9], q1[10], q1[11],
			q1[12], q1[13], q1[14], q1[15], q1[16], q1[17], q1[18], q1[19], q1[20], q1[21], q1[22], q1[23],
			q2[0], q2[1], q2[2], q2[3], q2[4], q2[5], q2[6], q2[7], q2[8], q2[9], q2[10], q2[11],
			q2[12], q2[13], q2[14], q2[15], q2[16], q2[17], q2[18], q2[19], q2[20], q2[21], q2[22], q2[23],
			q3[0], q3[1], q3[2], q3[3], q3[4], q3[5], q3[6], q3[7], q3[8], q3[9], q3[10], q3[11],
			q3[12], q3[13], q3[14], q3[15], q3[16], q3[17], q3[18], q3[19], q3[20], q3[21], q3[22], q3[23],
			l[0], l[1], l[2], l[3], l[4], l[5], l[6], l[7], l[8], l[9], l[10], l[11],
			l[12], l[13], l[14], l[15], l[16], l[17], l[18], l[19], l[20], l[21], l[22], l[23],
			s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7], s[8], s[9], s[10], s[11],
			s[12], s[13], s[14], s[15], s[16], s[17], s[18], s[19], s[20], s[21], s[22], s[23],
			totalIncomingMessagesDelta,
		).Scan(&lastErrors, &lastHourWithExamples, &id)
	}

	if nil != err {
		fmt.Printf("forwarder.forwardDb.UpdateUsage() Failed to upsert: %v\n", err)
		return -1, -1, hourNow, err
	}

	if len(lastErrors) < 10 && "" != errorMessage {
		lastErrors = append(lastErrors, errorMessage)
		fmt.Printf("want to appendl: %#v\n", lastErrors)

		counter := 1
		var args []interface{}

		q = "update webhook_forwarder_daily_forward_stats set last_errors = Array["
		for _, errm := range lastErrors {
			if 1 < counter {
				q += ","
			}
			q += "$" + strconv.Itoa(counter)
			runes := []rune(errm)
			if len(runes) > 99 {
				errm = string(runes[0:99])
			}
			args = append(args, errm)
			counter ++
		}
		q += "]::varchar[] where company_id = $" + strconv.Itoa(counter) + " and created_at = $" + strconv.Itoa(counter+1)
		counter += 2
		args = append(args, companyId)
		args = append(args, day)
		_, err := dbconn.Exec(context.Background(), q, args...)
		if err != nil {
			fmt.Printf("Failed to set last_errors: %#v  err:%v  q:%v\n", lastErrors, err, q)
		}
	}

	return id, lastHourWithExamples, hourNow, err
}

// UpdateUsageV2 returns id, hourNow, lastHourWithErrors, lastHourWithExamples, err
func UpdateUsageV2(companyId int, stats *forwarderStats.StatsV2) (int, int, int, int, error) {

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		fmt.Printf("asd90210 %v\n", err)
		return -1, -1, -1, -1, err
	}

	loc, _ := time.LoadLocation("UTC")
	t := time.Now().In(loc)
	hourNow := t.Hour()
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())

	r := stats.ReceivedAtH
	a := stats.AgeWhenForward
	f := stats.ForwardedAtH

	// First we try a normal update, and if that fails we do an upsert.
	q := `
UPDATE webhook_forwarder_daily_forward_stats_v2 as o
SET 
    rec_h00 = o.rec_h00 + $1,  rec_h01 = o.rec_h01 + $2,  rec_h02 = o.rec_h02 + $3,  rec_h03 = o.rec_h03 + $4,
    rec_h04 = o.rec_h04 + $5,  rec_h05 = o.rec_h05 + $6,  rec_h06 = o.rec_h06 + $7,  rec_h07 = o.rec_h07 + $8,
    rec_h08 = o.rec_h08 + $9,  rec_h09 = o.rec_h09 + $10, rec_h10 = o.rec_h10 + $11, rec_h11 = o.rec_h11 + $12,
    rec_h12 = o.rec_h12 + $13, rec_h13 = o.rec_h13 + $14, rec_h14 = o.rec_h14 + $15, rec_h15 = o.rec_h15 + $16,
    rec_h16 = o.rec_h16 + $17, rec_h17 = o.rec_h17 + $18, rec_h18 = o.rec_h18 + $19, rec_h19 = o.rec_h19 + $20,
    rec_h20 = o.rec_h20 + $21, rec_h21 = o.rec_h21 + $22, rec_h22 = o.rec_h22 + $23, rec_h23 = o.rec_h23 + $24, 

    age_h00 = o.age_h00 + $25, age_h01 = o.age_h01 + $26, age_h02 = o.age_h02 + $27, age_h03 = o.age_h03 + $28,
    age_h04 = o.age_h04 + $29, age_h05 = o.age_h05 + $30, age_h06 = o.age_h06 + $31, age_h07 = o.age_h07 + $32,
    age_h08 = o.age_h08 + $33, age_h09 = o.age_h09 + $34, age_h10 = o.age_h10 + $35, age_h11 = o.age_h11 + $36,
    age_h12 = o.age_h12 + $37, age_h13 = o.age_h13 + $38, age_h14 = o.age_h14 + $39, age_h15 = o.age_h15 + $40,
    age_h16 = o.age_h16 + $41, age_h17 = o.age_h17 + $42, age_h18 = o.age_h18 + $43, age_h19 = o.age_h19 + $44,
    age_h20 = o.age_h20 + $45, age_h21 = o.age_h21 + $46, age_h22 = o.age_h22 + $47, age_h23 = o.age_h23 + $48, 

    fwd_h00 = o.fwd_h00 + $49, fwd_h01 = o.fwd_h01 + $50, fwd_h02 = o.fwd_h02 + $51, fwd_h03 = o.fwd_h03 + $52,
    fwd_h04 = o.fwd_h04 + $53, fwd_h05 = o.fwd_h05 + $54, fwd_h06 = o.fwd_h06 + $55, fwd_h07 = o.fwd_h07 + $56,
    fwd_h08 = o.fwd_h08 + $57, fwd_h09 = o.fwd_h09 + $58, fwd_h10 = o.fwd_h10 + $59, fwd_h11 = o.fwd_h11 + $60,
    fwd_h12 = o.fwd_h12 + $61, fwd_h13 = o.fwd_h13 + $62, fwd_h14 = o.fwd_h14 + $63, fwd_h15 = o.fwd_h15 + $64,
    fwd_h16 = o.fwd_h16 + $65, fwd_h17 = o.fwd_h17 + $66, fwd_h18 = o.fwd_h18 + $67, fwd_h19 = o.fwd_h19 + $68,
    fwd_h20 = o.fwd_h20 + $69, fwd_h21 = o.fwd_h21 + $70, fwd_h22 = o.fwd_h22 + $71, fwd_h23 = o.fwd_h23 + $72, 

    total_lost_messages = o.total_lost_messages + $73
where
    company_id = $74 AND
    created_at = $75
returning circular_pointer_0to3, last_hour_with_errors, last_hour_with_examples, id
`
	var circularPointer0to3 int
	var lastHourWithErrors int
	var lastHourWithExamples int
	var id int

	errUpdate := dbconn.QueryRow(context.Background(), q,
		r[0],  r[1],  r[2],  r[3],  r[4],  r[5],  r[6],  r[7],  r[8],  r[9],  r[10], r[11],
		r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23],
		a[0],  a[1],  a[2],  a[3],  a[4],  a[5],  a[6],  a[7],  a[8],  a[9],  a[10], a[11],
		a[12], a[13], a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23],
		f[0],  f[1],  f[2],  f[3],  f[4],  f[5],  f[6],  f[7],  f[8],  f[9],  f[10], f[11],
		f[12], f[13], f[14], f[15], f[16], f[17], f[18], f[19], f[20], f[21], f[22], f[23],
		stats.NbrLostMessages,
		companyId, day).Scan(&circularPointer0to3, &lastHourWithErrors, &lastHourWithExamples, &id)

	if nil != errUpdate {
		q = `
INSERT INTO webhook_forwarder_daily_forward_stats_v2 as o (
    company_id,
    created_at, 
    rec_h00, rec_h01, rec_h02, rec_h03, rec_h04, rec_h05, rec_h06, rec_h07, rec_h08, rec_h09, rec_h10, rec_h11,
    rec_h12, rec_h13, rec_h14, rec_h15, rec_h16, rec_h17, rec_h18, rec_h19, rec_h20, rec_h21, rec_h22, rec_h23,
    age_h00, age_h01, age_h02, age_h03, age_h04, age_h05, age_h06, age_h07, age_h08, age_h09, age_h10, age_h11,
    age_h12, age_h13, age_h14, age_h15, age_h16, age_h17, age_h18, age_h19, age_h20, age_h21, age_h22, age_h23,
    fwd_h00, fwd_h01, fwd_h02, fwd_h03, fwd_h04, fwd_h05, fwd_h06, fwd_h07, fwd_h08, fwd_h09, fwd_h10, fwd_h11,
    fwd_h12, fwd_h13, fwd_h14, fwd_h15, fwd_h16, fwd_h17, fwd_h18, fwd_h19, fwd_h20, fwd_h21, fwd_h22, fwd_h23,
    circular_pointer_0to3,
    last_hour_with_errors,
    last_hour_with_examples,
    total_lost_messages
) values (
    $1,
    $2,
    $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
    $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
    $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74,
    $75,
    -1,
    -1,
    0
)
ON CONFLICT (company_id, created_at) DO UPDATE 
SET 
    rec_h00 = o.rec_h00 + $76, rec_h01 = o.rec_h01 + $77, rec_h02 = o.rec_h02 + $78, rec_h03 = o.rec_h03 + $79,
    rec_h04 = o.rec_h04 + $80, rec_h05 = o.rec_h05 + $81, rec_h06 = o.rec_h06 + $82, rec_h07 = o.rec_h07 + $83,
    rec_h08 = o.rec_h08 + $84, rec_h09 = o.rec_h09 + $85, rec_h10 = o.rec_h10 + $86, rec_h11 = o.rec_h11 + $87,
    rec_h12 = o.rec_h12 + $88, rec_h13 = o.rec_h13 + $89, rec_h14 = o.rec_h14 + $90, rec_h15 = o.rec_h15 + $91,
    rec_h16 = o.rec_h16 + $92, rec_h17 = o.rec_h17 + $93, rec_h18 = o.rec_h18 + $94, rec_h19 = o.rec_h19 + $95,
    rec_h20 = o.rec_h20 + $96, rec_h21 = o.rec_h21 + $97, rec_h22 = o.rec_h22 + $98, rec_h23 = o.rec_h23 + $99, 

    age_h00 = o.age_h00 + $100, age_h01 = o.age_h01 + $101, age_h02 = o.age_h02 + $102, age_h03 = o.age_h03 + $103,
    age_h04 = o.age_h04 + $104, age_h05 = o.age_h05 + $105, age_h06 = o.age_h06 + $106, age_h07 = o.age_h07 + $107,
    age_h08 = o.age_h08 + $108, age_h09 = o.age_h09 + $109, age_h10 = o.age_h10 + $110, age_h11 = o.age_h11 + $111,
    age_h12 = o.age_h12 + $112, age_h13 = o.age_h13 + $113, age_h14 = o.age_h14 + $114, age_h15 = o.age_h15 + $115,
    age_h16 = o.age_h16 + $116, age_h17 = o.age_h17 + $117, age_h18 = o.age_h18 + $118, age_h19 = o.age_h19 + $119,
    age_h20 = o.age_h20 + $120, age_h21 = o.age_h21 + $121, age_h22 = o.age_h22 + $122, age_h23 = o.age_h23 + $123, 

    fwd_h00 = o.fwd_h00 + $124, fwd_h01 = o.fwd_h01 + $125, fwd_h02 = o.fwd_h02 + $126, fwd_h03 = o.fwd_h03 + $127,
    fwd_h04 = o.fwd_h04 + $128, fwd_h05 = o.fwd_h05 + $129, fwd_h06 = o.fwd_h06 + $130, fwd_h07 = o.fwd_h07 + $131,
    fwd_h08 = o.fwd_h08 + $132, fwd_h09 = o.fwd_h09 + $133, fwd_h10 = o.fwd_h10 + $134, fwd_h11 = o.fwd_h11 + $135,
    fwd_h12 = o.fwd_h12 + $136, fwd_h13 = o.fwd_h13 + $137, fwd_h14 = o.fwd_h14 + $138, fwd_h15 = o.fwd_h15 + $139,
    fwd_h16 = o.fwd_h16 + $140, fwd_h17 = o.fwd_h17 + $141, fwd_h18 = o.fwd_h18 + $142, fwd_h19 = o.fwd_h19 + $143,
    fwd_h20 = o.fwd_h20 + $144, fwd_h21 = o.fwd_h21 + $145, fwd_h22 = o.fwd_h22 + $146, fwd_h23 = o.fwd_h23 + $147, 

    total_lost_messages = o.total_lost_messages + $148

returning circular_pointer_0to3, last_hour_with_errors, last_hour_with_examples, id
`
		err = dbconn.QueryRow(context.Background(), q, companyId, day,
			r[0],  r[1],  r[2],  r[3],  r[4],  r[5],  r[6],  r[7],  r[8],  r[9],  r[10], r[11],
			r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23],
			a[0],  a[1],  a[2],  a[3],  a[4],  a[5],  a[6],  a[7],  a[8],  a[9],  a[10], a[11],
			a[12], a[13], a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23],
			f[0],  f[1],  f[2],  f[3],  f[4],  f[5],  f[6],  f[7],  f[8],  f[9],  f[10], f[11],
			f[12], f[13], f[14], f[15], f[16], f[17], f[18], f[19], f[20], f[21], f[22], f[23],
			0,
			r[0],  r[1],  r[2],  r[3],  r[4],  r[5],  r[6],  r[7],  r[8],  r[9],  r[10], r[11],
			r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23],
			a[0],  a[1],  a[2],  a[3],  a[4],  a[5],  a[6],  a[7],  a[8],  a[9],  a[10], a[11],
			a[12], a[13], a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23],
			f[0],  f[1],  f[2],  f[3],  f[4],  f[5],  f[6],  f[7],  f[8],  f[9],  f[10], f[11],
			f[12], f[13], f[14], f[15], f[16], f[17], f[18], f[19], f[20], f[21], f[22], f[23],
			stats.NbrLostMessages,
		).Scan(&circularPointer0to3, &lastHourWithErrors, &lastHourWithExamples, &id)
	}

	if nil != err {
		fmt.Printf("forwarder.forwardDb.UpdateUsage() Failed to upsert: %v\n", err)
		return -1, hourNow, -1, -1, err
	}

	if "" != stats.ErrorMessage && hourNow != lastHourWithErrors {
		errm := stats.ErrorMessage
		runes := []rune(errm)
		if len(runes) > 99 {
			errm = string(runes[0:99])
		}

		q = fmt.Sprintf(`
        update webhook_forwarder_daily_forward_stats_v2
        set
            ex%d = $1,
            circular_pointer_0to3 = $2,
            last_hour_with_errors = $3
        where
            id = $4
        `, circularPointer0to3 + 1)
		_, err = dbconn.Exec(context.Background(), q, errm, (circularPointer0to3 + 1) & 0x3, hourNow, id)
		if err != nil {
			fmt.Printf("rabarberpaj %v\n", err)
			return -1, hourNow, -1, -1, err
		}
	}

	return id, hourNow, lastHourWithErrors, lastHourWithExamples, err
}

func WriteStatsToDbV2() (int, int) {
	var nbrForwarded int = 0
	var nbrLost int = 0
	for companyId, s := range forwarderStats.StatsMapV2 {
		for i:=0; i<24; i++ {
			nbrForwarded += s.ForwardedAtH[i]
		}
		nbrLost += s.NbrLostMessages

		id, hourNow, _, lastHourWithExamples, err := UpdateUsageV2(companyId, s)
		if nil == err {
			if "" != s.Example && hourNow != lastHourWithExamples {
				err = UpdateLastInMessageV2(companyId, s.Example, id, hourNow)
				if nil != err {
					fmt.Printf("forwarder.pg.WriteStatsToDbV2() UpdateLastInMessage(): %v\n", err)
				}
			}
		} else {
			fmt.Printf("forwarder.pg.WriteStatsToDbV2() %v\n", err)
		}
	}

	forwarderStats.CleanupV2()

	return nbrForwarded, nbrLost
}

func UpdateLastInMessageV2(companyId int, exampleMessage string, forwardStatsId int, hourNow int) error {
	if "" == exampleMessage {
		return fmt.Errorf("forwarder.database.UpdateLastInMessage(): Received an empty message")
	}

	if 999 < len(exampleMessage) {
		runes := []rune(exampleMessage)
		if len(runes) > 999 {
			exampleMessage = string(runes[0:999])
		}
	}

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return err
	}

	var q = `
    update webhook_forwarder_daily_forward_stats_v2
    set
        last_hour_with_examples = $1
    where
        id = $2
    `
	_, err = dbconn.Exec(context.Background(), q, hourNow, forwardStatsId)
	if err != nil {
		return err
	}

	var circularPointer0to3 int
	q = `
    SELECT
        circular_pointer_0to3
    FROM
        webhook_forwarder_latest_forward_examples
    WHERE
        company_id = $1
`
	err = dbconn.QueryRow(context.Background(), q, companyId).Scan(&circularPointer0to3)

	if nil != err {
		q = `
    INSERT INTO webhook_forwarder_latest_forward_examples as o
    (
        company_id,
        created_at,
        circular_pointer_0to3
    ) values (
        $1,
        now(),
        0
    )
    ON CONFLICT (company_id) DO UPDATE 
    SET 
        circular_pointer_0to3 = o.circular_pointer_0to3
    RETURNING circular_pointer_0to3
    `
		err = dbconn.QueryRow(context.Background(), q, companyId).Scan(&circularPointer0to3)
		if nil != err {
			return err
		}
	}

	exDest := 1 + circularPointer0to3
	circularPointer0to3 = (circularPointer0to3 + 1) & 3
	q = `
    UPDATE webhook_forwarder_latest_forward_examples
    SET
        circular_pointer_0to3 = $1,
        ex` + strconv.Itoa(exDest) + ` = $2
    WHERE
        company_id = $3
    `
	_, err = dbconn.Exec(context.Background(), q, circularPointer0to3, exampleMessage, companyId)
	return err
}

func UpdateLastInMessage(companyId int, exampleMessage string, forwardStatsId int, hourNow int) error {
	if "" == exampleMessage {
		return fmt.Errorf("forwarder.database.UpdateLastInMessage(): Received an empty message")
	}

	if 999 < len(exampleMessage) {
		runes := []rune(exampleMessage)
		if len(runes) > 999 {
			exampleMessage = string(runes[0:999])
		}
	}

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return err
	}

	var q = `
    update webhook_forwarder_daily_forward_stats
    set
        last_hour_with_examples = $1
    where
        id = $2
    `
	_, err = dbconn.Exec(context.Background(), q, hourNow, forwardStatsId)
	if err != nil {
		return err
	}

	var circularPointer0to3 int
	q = `
    SELECT
        circular_pointer_0to3
    FROM
        webhook_forwarder_latest_forward_examples
    WHERE
        company_id = $1
`
	err = dbconn.QueryRow(context.Background(), q, companyId).Scan(&circularPointer0to3)

	if nil != err {
		q = `
    INSERT INTO webhook_forwarder_latest_forward_examples as o
    (
        company_id,
        created_at,
        circular_pointer_0to3
    ) values (
        $1,
        now(),
        0
    )
    ON CONFLICT (company_id) DO UPDATE 
    SET 
        circular_pointer_0to3 = o.circular_pointer_0to3
    RETURNING circular_pointer_0to3
    `
		err = dbconn.QueryRow(context.Background(), q, companyId).Scan(&circularPointer0to3)
		if nil != err {
			return err
		}
	}

	exDest := 1 + circularPointer0to3
	circularPointer0to3 = (circularPointer0to3 + 1) & 3
	q = `
    UPDATE webhook_forwarder_latest_forward_examples
    SET
        circular_pointer_0to3 = $1,
        ex` + strconv.Itoa(exDest) + ` = $2
    WHERE
        company_id = $3
    `
	_, err = dbconn.Exec(context.Background(), q, circularPointer0to3, exampleMessage, companyId)
	return err
}

func Cleanup() {
	if globalDb != nil {
		err := globalDb.Close(context.Background())
		if nil != err {
			fmt.Printf("Forwarder.pg.Cleanup() Error closing db: %v\n", err)
		}
		globalDb = nil
	}
}
