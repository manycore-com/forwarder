package forwarderDb

import (
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx"
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

	connConfig, err := pgx.ParseConnectionString(dbURI)
	if err != nil {
		return nil, err
	}

	// dbPool is the pool of database connections.
	conn, err := pgx.Connect(connConfig)
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

		err = dbconn.QueryRow(q, companyId).Scan(&jsonStr, &secret)
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

	loc, _ := time.LoadLocation("UTC")
	t := time.Now().In(loc)
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

	// s = successfully received into Fanout
	// q1 = successful fanout writes
	// q2 = first forward attempt failed, and msg is put on q2 without errors
	// q3 = second forward attempt failed, and msg is put on q3 without errors
	// l = lost: too many retries / unrecoverable error
	q := `
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
	var lastErrors []string
	var lastHourWithExamples int
	var id int

	err = dbconn.QueryRow(q, companyId, day,
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

	if len(lastErrors) < 10 && "" != errorMessage {
		lastErrors = append(lastErrors, errorMessage)
		fmt.Printf("want to appendl: %#v\n", lastErrors)

		counter := 1
		args := []interface{}{}

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
		_, err := dbconn.Exec(q, args...)
		if err != nil {
			fmt.Printf("Failed to set last_errors: %#v  err:%v  q:%v\n", lastErrors, err, q)
		}
	}

	fmt.Printf("l: %#v\n", lastErrors)

	return id, lastHourWithExamples, hourNow, err
}

func UpdateLastInMessage(companyId int, errorMessage string, forwardStatsId int, hourNow int) error {
	if "" == errorMessage {
		return fmt.Errorf("forwarder.database.UpdateLastInMessage(): Received an empty message")
	}

	if 999 < len(errorMessage) {
		runes := []rune(errorMessage)
		if len(runes) > 999 {
			errorMessage = string(runes[0:999])
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
	_, err = dbconn.Exec(q, hourNow, forwardStatsId)
	if err != nil {
		return err
	}

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
	var circularPointer0to3 int
	err = dbconn.QueryRow(q, companyId).Scan(&circularPointer0to3)
	if nil != err {
		return err
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
	_, err = dbconn.Exec(q, circularPointer0to3, errorMessage, companyId)
	return err
}

func Cleanup() {
	if globalDb != nil {
		err := globalDb.Close()
		if nil != err {
			fmt.Printf("Forwarder.pg.Cleanup() Error closing db: %v", err)
		}
		globalDb = nil
	}
}
