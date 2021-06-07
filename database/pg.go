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

// UpdateUsage returns id, hourNow, lastHourWithErrors, lastHourWithExamples, err
func UpdateUsage(companyId int, stats *forwarderStats.Stats) (int, int, int, int, error) {

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

    total_lost_messages = o.total_lost_messages + $73,
    total_timeout_messages = o.total_timeout_messages + $74
where
    company_id = $75 AND
    created_at = $76
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
		stats.NbrLost, stats.NbrTimeout,
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
    total_lost_messages,
    total_timeout_messages
) values (
    $1,
    $2,
    $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
    $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50,
    $51, $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74,
    $75,
    -1,
    -1,
    0,
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

    total_lost_messages = o.total_lost_messages + $148,
    total_timeout_messages = o.total_timeout_messages + $149

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
			stats.NbrLost, stats.NbrTimeout,
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

func WriteStatsToDb() (int, int) {
	var nbrForwarded int = 0
	var nbrLost int = 0
	for companyId, s := range forwarderStats.StatsMap {
		for i:=0; i<24; i++ {
			nbrForwarded += s.ForwardedAtH[i]
		}
		nbrLost += s.NbrLost

		id, hourNow, _, lastHourWithExamples, err := UpdateUsage(companyId, s)
		if nil == err {
			if "" != s.Example && hourNow != lastHourWithExamples {
				err = UpdateLastInMessage(companyId, s.Example, id, hourNow)
				if nil != err {
					fmt.Printf("forwarder.pg.WriteStatsToDb() UpdateLastInMessage(): %v\n", err)
				}
			}
		} else {
			fmt.Printf("forwarder.pg.WriteStatsToDb() %v\n", err)
		}
	}

	forwarderStats.CleanupV2()

	return nbrForwarded, nbrLost
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

func Cleanup() {
	if globalDb != nil {
		err := globalDb.Close(context.Background())
		if nil != err {
			fmt.Printf("Forwarder.pg.Cleanup() Error closing db: %v\n", err)
		}
		globalDb = nil
	}
	companyInfoMap = make(map[int]*CompanyInfo)
}
