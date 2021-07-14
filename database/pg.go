package forwarderDb

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4"
	"github.com/lib/pq"
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

type EndPointStruct struct {
	EndPointId          int
	ForwardEndPoint     string
	MaxConcurrentFwd    int
}

type CompanyInfo struct {
	EndPoints           []EndPointStruct
	Secret              string
	WarnedAt            pq.NullTime  // Can't Scan() null into normal *time.Time
	DisabledAt          pq.NullTime
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

func IsPaused(hashId int) bool {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		fmt.Printf("forwarder.database.IsPaused() Failed to get connection: %v\n", err)
		return false
	}

	var nbrRows int
	q := `
    select count(*) as nbrRows 
    from webhook_pause 
    where hash_id = $1
    `
	err = dbconn.QueryRow(context.Background(), q, hashId).Scan(&nbrRows)
	if nil != err {
		fmt.Printf("forwarder.database.IsPaused() Failed to get connection: %v\n", err)
		return false
	}

	return 0 != nbrRows
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
		var warnedAt pq.NullTime
		var disabledAt pq.NullTime
		q := `
        select 
            coalesce((
                select array_to_json(array_agg(row_to_json(t)))
                from (
                    select 
                        id as "EndPointId",
                        forward_endpoint as "ForwardEndPoint",
                        coalesce(max_concurrent_fwd,1000000) as "MaxConcurrentFwd"
                    from
                        webhook_forwarder_poll_endpoint ipe
                    where
                        ipe.company_id = ipc.company_id AND
                        ipe.is_active = true
                ) t
            ),'[]') as json,
            secret,
            warned_at,
            disabled_at
        from 
            webhook_forwarder_poll_cfg ipc
        where company_id = $1
        `

		err = dbconn.QueryRow(context.Background(), q, companyId).Scan(&jsonStr, &secret, &warnedAt, &disabledAt)
		if err != nil {
			if strings.Contains(fmt.Sprintf("%v", err), "no rows in result set") {
				companyInfoMap[companyId] = nil
				return nil, nil
			}

			companyInfoMap[companyId] = nil
			return nil, err
		}

		var ci = CompanyInfo{Secret: secret, WarnedAt: warnedAt, DisabledAt: disabledAt}
		companyInfoMap[companyId] = &ci
		theElem = &ci

		var jsonRows []EndPointStruct
		if "" != jsonStr && "[]" != jsonStr {
			err = json.Unmarshal([]byte(jsonStr), &jsonRows)
			if err != nil {
				companyInfoMap[companyId] = nil
				return nil, err
			}

			ci.EndPoints = jsonRows
		}
	}

	return theElem, nil
}

type EndPointCfgStruct struct {
	MaxConcurrentFwd    int
	ForwardEndpoint     string
}

func GetEndPointCfg(endPointId int) (*EndPointCfgStruct, error) {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return nil, err
	}

	var maxConcurrentFwd int
	var forwardEndpoint string

	q := `
    select
        coalesce(max_concurrent_fwd, 1000000),
	    forward_endpoint
    from
        webhook_forwarder_poll_endpoint wfpe
    where
        wfpe.id = $1
    `
	err = dbconn.QueryRow(context.Background(), q, endPointId).Scan(&maxConcurrentFwd, &forwardEndpoint)
	if err != nil {
		return nil, fmt.Errorf("forwarder.database.GetEndPointCfg() Error:%v", err)
	}

	var val = EndPointCfgStruct{
		MaxConcurrentFwd: maxConcurrentFwd,
		ForwardEndpoint: forwardEndpoint,
	}

	return &val, nil
}

// UpdateUsage returns id, hourNow, lastHourWithErrors, lastHourWithExamples, err
func UpdateUsage(endpointId int, stats *forwarderStats.Stats) (int, int, int, int, error) {

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return -1, -1, -1, -1, err
	}

	t := time.Now().UTC()
	hourNow := t.Hour()
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	r := stats.ReceivedAtH
	a := [24]int{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}
	f := stats.ForwardedAtH
	e := stats.EnterQueueAtH

	for i:=0; i<24; i++ {
		a[i] = stats.AgeWhenForward[forwarderStats.TruncatedHourList[i]]
	}

	// First we try a normal update, and if that fails we do an upsert.
	q := `
UPDATE webhook_forwarder_daily_forward_stats_v3 as o
SET 
    rec_h00 = o.rec_h00 + $1,  rec_h01 = o.rec_h01 + $2,  rec_h02 = o.rec_h02 + $3,  rec_h03 = o.rec_h03 + $4,
    rec_h04 = o.rec_h04 + $5,  rec_h05 = o.rec_h05 + $6,  rec_h06 = o.rec_h06 + $7,  rec_h07 = o.rec_h07 + $8,
    rec_h08 = o.rec_h08 + $9,  rec_h09 = o.rec_h09 + $10, rec_h10 = o.rec_h10 + $11, rec_h11 = o.rec_h11 + $12,
    rec_h12 = o.rec_h12 + $13, rec_h13 = o.rec_h13 + $14, rec_h14 = o.rec_h14 + $15, rec_h15 = o.rec_h15 + $16,
    rec_h16 = o.rec_h16 + $17, rec_h17 = o.rec_h17 + $18, rec_h18 = o.rec_h18 + $19, rec_h19 = o.rec_h19 + $20,
    rec_h20 = o.rec_h20 + $21, rec_h21 = o.rec_h21 + $22, rec_h22 = o.rec_h22 + $23, rec_h23 = o.rec_h23 + $24, 

    age_h00 = coalesce(o.age_h00,0) + $25, age_h01 = coalesce(o.age_h01,0) + $26, age_h02 = coalesce(o.age_h02,0) + $27, age_h03 = coalesce(o.age_h03,0) + $28, 
    age_h04 = coalesce(o.age_h04,0) + $29, age_h05 = coalesce(o.age_h05,0) + $30, age_h06 = coalesce(o.age_h06,0) + $31, age_h07 = coalesce(o.age_h07,0) + $32,
    age_h08 = coalesce(o.age_h08,0) + $33, age_h09 = coalesce(o.age_h09,0) + $34, age_h10 = coalesce(o.age_h10,0) + $35, age_h11 = coalesce(o.age_h11,0) + $36,
    age_h12 = coalesce(o.age_h12,0) + $37, age_h16 = coalesce(o.age_h16,0) + $38, age_h20 = coalesce(o.age_h20,0) + $39, age_h24 = coalesce(o.age_h24,0) + $40,
    age_h28 = coalesce(o.age_h28,0) + $41, age_h32 = coalesce(o.age_h32,0) + $42, age_h36 = coalesce(o.age_h36,0) + $43, age_h42 = coalesce(o.age_h42,0) + $44,
    age_h48 = coalesce(o.age_h48,0) + $45, age_h54 = coalesce(o.age_h54,0) + $46, age_h60 = coalesce(o.age_h60,0) + $47, age_h66 = coalesce(o.age_h66,0) + $48,

    fwd_h00 = o.fwd_h00 + $49, fwd_h01 = o.fwd_h01 + $50, fwd_h02 = o.fwd_h02 + $51, fwd_h03 = o.fwd_h03 + $52,
    fwd_h04 = o.fwd_h04 + $53, fwd_h05 = o.fwd_h05 + $54, fwd_h06 = o.fwd_h06 + $55, fwd_h07 = o.fwd_h07 + $56,
    fwd_h08 = o.fwd_h08 + $57, fwd_h09 = o.fwd_h09 + $58, fwd_h10 = o.fwd_h10 + $59, fwd_h11 = o.fwd_h11 + $60,
    fwd_h12 = o.fwd_h12 + $61, fwd_h13 = o.fwd_h13 + $62, fwd_h14 = o.fwd_h14 + $63, fwd_h15 = o.fwd_h15 + $64,
    fwd_h16 = o.fwd_h16 + $65, fwd_h17 = o.fwd_h17 + $66, fwd_h18 = o.fwd_h18 + $67, fwd_h19 = o.fwd_h19 + $68,
    fwd_h20 = o.fwd_h20 + $69, fwd_h21 = o.fwd_h21 + $70, fwd_h22 = o.fwd_h22 + $71, fwd_h23 = o.fwd_h23 + $72, 

    ent_h00 = o.ent_h00 + $73, ent_h01 = o.ent_h01 + $74, ent_h02 = o.ent_h02 + $75, ent_h03 = o.ent_h03 + $76,
    ent_h04 = o.ent_h04 + $77, ent_h05 = o.ent_h05 + $78, ent_h06 = o.ent_h06 + $79, ent_h07 = o.ent_h07 + $80,
    ent_h08 = o.ent_h08 + $81, ent_h09 = o.ent_h09 + $82, ent_h10 = o.ent_h10 + $83, ent_h11 = o.ent_h11 + $84,
    ent_h12 = o.ent_h12 + $85, ent_h13 = o.ent_h13 + $86, ent_h14 = o.ent_h14 + $87, ent_h15 = o.ent_h15 + $88,
    ent_h16 = o.ent_h16 + $89, ent_h17 = o.ent_h17 + $90, ent_h18 = o.ent_h18 + $91, ent_h19 = o.ent_h19 + $92,
    ent_h20 = o.ent_h20 + $93, ent_h21 = o.ent_h21 + $94, ent_h22 = o.ent_h22 + $95, ent_h23 = o.ent_h23 + $96,

    total_lost_messages = o.total_lost_messages + $97,
    total_timeout_messages = o.total_timeout_messages + $98
where
    endpoint_id = $99 AND
    company_id = $100 AND
    created_at = $101
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
		e[0],  e[1],  e[2],  e[3],  e[4],  e[5],  e[6],  e[7],  e[8],  e[9],  e[10], e[11],
		e[12], e[13], e[14], e[15], e[16], e[17], e[18], e[19], e[20], e[21], e[22], e[23],
		stats.NbrLost, stats.NbrTimeout,
		endpointId, stats.CompanyId, day).Scan(&circularPointer0to3, &lastHourWithErrors, &lastHourWithExamples, &id)

	if nil != errUpdate {
		fmt.Printf("forwarder.forwardDb.UpdateUsage() Failed to update, will try upsert: %v\n", errUpdate)

		q = `
INSERT INTO webhook_forwarder_daily_forward_stats_v3 as o (
    endpoint_id,
    company_id,
    created_at, 
    rec_h00, rec_h01, rec_h02, rec_h03, rec_h04, rec_h05, rec_h06, rec_h07, rec_h08, rec_h09, rec_h10, rec_h11,
    rec_h12, rec_h13, rec_h14, rec_h15, rec_h16, rec_h17, rec_h18, rec_h19, rec_h20, rec_h21, rec_h22, rec_h23,
    age_h00, age_h01, age_h02, age_h03, age_h04, age_h05, age_h06, age_h07, age_h08, age_h09, age_h10, age_h11,  -- note stepping
    age_h12, age_h16, age_h20, age_h24, age_h28, age_h32, age_h36, age_h42, age_h48, age_h54, age_h60, age_h66,
    fwd_h00, fwd_h01, fwd_h02, fwd_h03, fwd_h04, fwd_h05, fwd_h06, fwd_h07, fwd_h08, fwd_h09, fwd_h10, fwd_h11,
    fwd_h12, fwd_h13, fwd_h14, fwd_h15, fwd_h16, fwd_h17, fwd_h18, fwd_h19, fwd_h20, fwd_h21, fwd_h22, fwd_h23,
    ent_h00, ent_h01, ent_h02, ent_h03, ent_h04, ent_h05, ent_h06, ent_h07, ent_h08, ent_h09, ent_h10, ent_h11,
    ent_h12, ent_h13, ent_h14, ent_h15, ent_h16, ent_h17, ent_h18, ent_h19, ent_h20, ent_h21, ent_h22, ent_h23,
    circular_pointer_0to3,
    last_hour_with_errors,
    last_hour_with_examples,
    total_lost_messages,
    total_timeout_messages,
    qus_h00, qus_h01, qus_h02, qus_h03, qus_h04, qus_h05, qus_h06, qus_h07, qus_h08, qus_h09, qus_h10, qus_h11,
    qus_h12, qus_h13, qus_h14, qus_h15, qus_h16, qus_h17, qus_h18, qus_h19, qus_h20, qus_h21, qus_h22, qus_h23
) values (
    $1,
    $2,
    $3,
    $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27,
    $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42, $43, $44, $45, $46, $47, $48, $49, $50, $51,
    $52, $53, $54, $55, $56, $57, $58, $59, $60, $61, $62, $63, $64, $65, $66, $67, $68, $69, $70, $71, $72, $73, $74, $75,
    $76, $77, $78, $79, $80, $81, $82, $83, $84, $85, $86, $87, $88, $89, $90, $91, $92, $93, $94, $95, $96, $97, $98, $99,
    $100,
    -1,
    -1,
    $101,
    $102,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
)
ON CONFLICT (endpoint_id, company_id, created_at) DO UPDATE 
SET 
    rec_h00 = o.rec_h00 + $103, rec_h01 = o.rec_h01 + $104, rec_h02 = o.rec_h02 + $105, rec_h03 = o.rec_h03 + $106,
    rec_h04 = o.rec_h04 + $107, rec_h05 = o.rec_h05 + $108, rec_h06 = o.rec_h06 + $109, rec_h07 = o.rec_h07 + $110,
    rec_h08 = o.rec_h08 + $111, rec_h09 = o.rec_h09 + $112, rec_h10 = o.rec_h10 + $113, rec_h11 = o.rec_h11 + $114,
    rec_h12 = o.rec_h12 + $115, rec_h13 = o.rec_h13 + $116, rec_h14 = o.rec_h14 + $117, rec_h15 = o.rec_h15 + $118,
    rec_h16 = o.rec_h16 + $119, rec_h17 = o.rec_h17 + $120, rec_h18 = o.rec_h18 + $121, rec_h19 = o.rec_h19 + $122,
    rec_h20 = o.rec_h20 + $123, rec_h21 = o.rec_h21 + $124, rec_h22 = o.rec_h22 + $125, rec_h23 = o.rec_h23 + $126, 

    age_h00 = o.age_h00 + $127, age_h01 = o.age_h01 + $128, age_h02 = o.age_h02 + $129, age_h03 = o.age_h03 + $130,  -- note stepping
    age_h04 = o.age_h04 + $131, age_h05 = o.age_h05 + $132, age_h06 = o.age_h06 + $133, age_h07 = o.age_h07 + $134,
    age_h08 = o.age_h08 + $135, age_h09 = o.age_h09 + $136, age_h10 = o.age_h10 + $137, age_h11 = o.age_h11 + $138,
    age_h12 = o.age_h12 + $139, age_h16 = o.age_h16 + $140, age_h20 = o.age_h20 + $141, age_h24 = o.age_h24 + $142,
    age_h28 = o.age_h28 + $143, age_h32 = o.age_h32 + $144, age_h36 = o.age_h36 + $145, age_h42 = o.age_h42 + $146,
    age_h48 = o.age_h48 + $147, age_h54 = o.age_h54 + $148, age_h60 = o.age_h60 + $149, age_h66 = o.age_h66 + $150, 

    fwd_h00 = o.fwd_h00 + $151, fwd_h01 = o.fwd_h01 + $152, fwd_h02 = o.fwd_h02 + $153, fwd_h03 = o.fwd_h03 + $154,
    fwd_h04 = o.fwd_h04 + $155, fwd_h05 = o.fwd_h05 + $156, fwd_h06 = o.fwd_h06 + $157, fwd_h07 = o.fwd_h07 + $158,
    fwd_h08 = o.fwd_h08 + $159, fwd_h09 = o.fwd_h09 + $160, fwd_h10 = o.fwd_h10 + $161, fwd_h11 = o.fwd_h11 + $162,
    fwd_h12 = o.fwd_h12 + $163, fwd_h13 = o.fwd_h13 + $164, fwd_h14 = o.fwd_h14 + $165, fwd_h15 = o.fwd_h15 + $166,
    fwd_h16 = o.fwd_h16 + $167, fwd_h17 = o.fwd_h17 + $168, fwd_h18 = o.fwd_h18 + $169, fwd_h19 = o.fwd_h19 + $170,
    fwd_h20 = o.fwd_h20 + $171, fwd_h21 = o.fwd_h21 + $172, fwd_h22 = o.fwd_h22 + $173, fwd_h23 = o.fwd_h23 + $174, 

    ent_h00 = o.ent_h00 + $175, ent_h01 = o.ent_h01 + $176, ent_h02 = o.ent_h02 + $177, ent_h03 = o.ent_h03 + $178,
    ent_h04 = o.ent_h04 + $179, ent_h05 = o.ent_h05 + $180, ent_h06 = o.ent_h06 + $181, ent_h07 = o.ent_h07 + $182,
    ent_h08 = o.ent_h08 + $183, ent_h09 = o.ent_h09 + $184, ent_h10 = o.ent_h10 + $185, ent_h11 = o.ent_h11 + $186,
    ent_h12 = o.ent_h12 + $187, ent_h13 = o.ent_h13 + $188, ent_h14 = o.ent_h14 + $189, ent_h15 = o.ent_h15 + $190,
    ent_h16 = o.ent_h16 + $191, ent_h17 = o.ent_h17 + $192, ent_h18 = o.ent_h18 + $193, ent_h19 = o.ent_h19 + $194,
    ent_h20 = o.ent_h20 + $195, ent_h21 = o.ent_h21 + $196, ent_h22 = o.ent_h22 + $197, ent_h23 = o.ent_h23 + $198,

    total_lost_messages = o.total_lost_messages + $199,
    total_timeout_messages = o.total_timeout_messages + $200

returning circular_pointer_0to3, last_hour_with_errors, last_hour_with_examples, id
`
		err = dbconn.QueryRow(context.Background(), q, endpointId, stats.CompanyId, day,
			r[0],  r[1],  r[2],  r[3],  r[4],  r[5],  r[6],  r[7],  r[8],  r[9],  r[10], r[11],
			r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23],
			a[0],  a[1],  a[2],  a[3],  a[4],  a[5],  a[6],  a[7],  a[8],  a[9],  a[10], a[11],
			a[12], a[13], a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23],
			f[0],  f[1],  f[2],  f[3],  f[4],  f[5],  f[6],  f[7],  f[8],  f[9],  f[10], f[11],
			f[12], f[13], f[14], f[15], f[16], f[17], f[18], f[19], f[20], f[21], f[22], f[23],
			e[0],  e[1],  e[2],  e[3],  e[4],  e[5],  e[6],  e[7],  e[8],  e[9],  e[10], e[11],
			e[12], e[13], e[14], e[15], e[16], e[17], e[18], e[19], e[20], e[21], e[22], e[23],
			0,
			stats.NbrLost, stats.NbrTimeout,
			r[0],  r[1],  r[2],  r[3],  r[4],  r[5],  r[6],  r[7],  r[8],  r[9],  r[10], r[11],
			r[12], r[13], r[14], r[15], r[16], r[17], r[18], r[19], r[20], r[21], r[22], r[23],
			a[0],  a[1],  a[2],  a[3],  a[4],  a[5],  a[6],  a[7],  a[8],  a[9],  a[10], a[11],
			a[12], a[13], a[14], a[15], a[16], a[17], a[18], a[19], a[20], a[21], a[22], a[23],
			f[0],  f[1],  f[2],  f[3],  f[4],  f[5],  f[6],  f[7],  f[8],  f[9],  f[10], f[11],
			f[12], f[13], f[14], f[15], f[16], f[17], f[18], f[19], f[20], f[21], f[22], f[23],
			e[0],  e[1],  e[2],  e[3],  e[4],  e[5],  e[6],  e[7],  e[8],  e[9],  e[10], e[11],
			e[12], e[13], e[14], e[15], e[16], e[17], e[18], e[19], e[20], e[21], e[22], e[23],
			stats.NbrLost,
			stats.NbrTimeout,
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
        update webhook_forwarder_daily_forward_stats_v3
        set
            ex%d = $1,
            circular_pointer_0to3 = $2,
            last_hour_with_errors = $3
        where
            id = $4
        `, circularPointer0to3 + 1)
		_, err = dbconn.Exec(context.Background(), q, errm, (circularPointer0to3 + 1) & 0x3, hourNow, id)
		if err != nil {
			fmt.Printf("forwarder.forwardDb.UpdateUsage() Failed to update stats.exX %v\n", err)
			return -1, hourNow, -1, -1, err
		}
	}

	return id, hourNow, lastHourWithErrors, lastHourWithExamples, err
}

func WriteStatsToDb() (int, int, int, int) {
	var nbrReceived int = 0
	var nbrForwarded int = 0
	var nbrLost int = 0
	var nbrTimeout int = 0
	for endpointId, s := range forwarderStats.StatsMap {
		for i:=0; i<24; i++ {
			nbrReceived += s.ReceivedAtH[i]
			nbrForwarded += s.ForwardedAtH[i]
		}
		nbrLost += s.NbrLost
		nbrTimeout += s.NbrTimeout

		id, hourNow, _, lastHourWithExamples, err := UpdateUsage(endpointId, s)
		if nil == err {
			if "" != s.Example && hourNow != lastHourWithExamples {
				err = UpdateLastInMessage(s.CompanyId, endpointId, s.Example, id, hourNow)
				if nil != err {
					fmt.Printf("forwarder.pg.WriteStatsToDb() UpdateLastInMessage(): %v\n", err)
				}
			}
		} else {
			fmt.Printf("forwarder.pg.WriteStatsToDb() %v\n", err)
		}
	}

	forwarderStats.CleanupV2()

	return nbrReceived, nbrForwarded, nbrLost, nbrTimeout
}

func UpdateLastInMessage(companyId int, endpointId int, exampleMessage string, forwardStatsId int, hourNow int) error {
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
    update webhook_forwarder_daily_forward_stats_v3
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
        webhook_forwarder_latest_forward_examples2
    WHERE
        endpoint_id = $1
`
	err = dbconn.QueryRow(context.Background(), q, endpointId).Scan(&circularPointer0to3)

	if nil != err {
		q = `
    INSERT INTO webhook_forwarder_latest_forward_examples2 as o
    (
        company_id,
        endpoint_id,
        created_at,
        circular_pointer_0to3
    ) values (
        $1,
        $2,
        now(),
        0
    )
    ON CONFLICT (endpoint_id) DO UPDATE 
    SET 
        circular_pointer_0to3 = o.circular_pointer_0to3
    RETURNING circular_pointer_0to3
    `
		err = dbconn.QueryRow(context.Background(), q, companyId, endpointId).Scan(&circularPointer0to3)
		if nil != err {
			return err
		}
	}

	exDest := 1 + circularPointer0to3
	circularPointer0to3 = (circularPointer0to3 + 1) & 3
	q = `
    UPDATE webhook_forwarder_latest_forward_examples2
    SET
        circular_pointer_0to3 = $1,
        ex` + strconv.Itoa(exDest) + ` = $2
    WHERE
        endpoint_id = $3
    `
	_, err = dbconn.Exec(context.Background(), q, circularPointer0to3, exampleMessage, endpointId)
	return err
}

func DeleteAPauseRow(hashId int) error {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if err != nil {
		return err
	}

	q := `
    delete from webhook_pause where hash_id = $1
    `

	_, err = dbconn.Exec(context.Background(), q, hashId)
	if nil != err {
		return err
	}

	return nil
}

func DeleteAllPauseRows() error {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if err != nil {
		return err
	}

	q := `
    delete from webhook_pause
    `

	_, err = dbconn.Exec(context.Background(), q)
	if nil != err {
		return err
	}

	return nil
}

func CreatePauseRows(nbrHash int) error {

	// DeleteAllPauseRows locks the db mutex too.
	err := DeleteAllPauseRows()
	if nil != err {
		return err
	}

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if err != nil {
		return err
	}

	u := time.Now().UTC()
	createdAt := time.Date(u.Year(), u.Month(), u.Day(), u.Hour(), 0, 0, 0, time.UTC)

	for i:=0; i<nbrHash; i++ {
		q := `
        insert into webhook_pause (
            hash_id,
            created_at
        ) values (
            $1,
            $2
        )
        `
		_, err = dbconn.Exec(context.Background(), q, i, createdAt)
		if err != nil {
			return err
		}
	}

	return nil
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

func WriteQueueCheckpoint(endpointId int, queueSize int) error {

	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return err
	}


	t := time.Now().UTC()
	created_at := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	var rec int
	var age int
	var fwd int
	var ent int
	var total_lost_messages int
	var total_timeout_messages int
	var companyId int
	q := `
    select
        rec_h00 + rec_h01 + rec_h02 + rec_h03 + rec_h04 + rec_h05 + rec_h06 + rec_h07 + rec_h08 + rec_h09 + rec_h10 + rec_h11 + rec_h12 + rec_h13 + rec_h14 + rec_h15 + rec_h16 + rec_h17 + rec_h18 + rec_h19 + rec_h20 + rec_h21 + rec_h22 + rec_h23 as sum_rec,
	    coalesce(age_h00,0) + coalesce(age_h01,0) + coalesce(age_h02,0) + coalesce(age_h03,0) + coalesce(age_h04,0) + coalesce(age_h05,0) + coalesce(age_h06,0) + coalesce(age_h07,0) + coalesce(age_h08,0) + coalesce(age_h09,0) + coalesce(age_h10,0) + coalesce(age_h11,0) + coalesce(age_h12,0) + coalesce(age_h16,0) + coalesce(age_h20,0) + coalesce(age_h24,0) + coalesce(age_h28,0) + coalesce(age_h32,0) + coalesce(age_h36,0) + coalesce(age_h42,0) + coalesce(age_h48,0) + coalesce(age_h54,0) + coalesce(age_h60,0) + coalesce(age_h66,0) as sum_age,
        fwd_h00 + fwd_h01 + fwd_h02 + fwd_h03 + fwd_h04 + fwd_h05 + fwd_h06 + fwd_h07 + fwd_h08 + fwd_h09 + fwd_h10 + fwd_h11 + fwd_h12 + fwd_h13 + fwd_h14 + fwd_h15 + fwd_h16 + fwd_h17 + fwd_h18 + fwd_h19 + fwd_h20 + fwd_h21 + fwd_h22 + fwd_h23 as sum_fwd,
        ent_h00 + ent_h01 + ent_h02 + ent_h03 + ent_h04 + ent_h05 + ent_h06 + ent_h07 + ent_h08 + ent_h09 + ent_h10 + ent_h11 + ent_h12 + ent_h13 + ent_h14 + ent_h15 + ent_h16 + ent_h17 + ent_h18 + ent_h19 + ent_h20 + ent_h21 + ent_h22 + ent_h23 as sum_ent,
        total_lost_messages as total_lost_messages,
        total_timeout_messages as total_timeout_messages,
        company_id
    from 
        webhook_forwarder_daily_forward_stats_v3
    where
        endpoint_id = $1 AND
        created_at = $2
    `
	err = dbconn.QueryRow(context.Background(), q, endpointId, created_at).Scan(&rec, &age, &fwd, &ent, &total_lost_messages, &total_timeout_messages, &companyId)
	if err != nil {
		// TODO what's the right way to check if no rows OR query failed?
		if ! strings.Contains(err.Error(), "no rows in ") {
			return err
		}

		fmt.Printf("forwarder.database.WriteQueueCheckpoint(): no stats row, assuming zeros\n")
	}

	// No rows found, we need to lookup companyId
	if 0 == companyId {
		q = `
        select
            company_id
        from
            webhook_forwarder_poll_endpoint
        where
            id = $1
        `
		err = dbconn.QueryRow(context.Background(), q, endpointId).Scan(&companyId)
		if err != nil {
			return fmt.Errorf("forwarder.database.WriteQueueCheckpoint() Failed to lookup Company from Endpoint=%d: %v", endpointId, err)
		}
	}

	q = `
    insert into webhook_queue_size_checkpoint_v2 as o (
        created_at,
        rec,
        ent,
        age,
        fwd,
        total_lost_messages,
        total_timeout_messages,
        counted_items_on_fwd_queues,
        company_id,
        endpoint_id
    ) values (
        $1,
        $2,
        $3,
        $4,
        $5,
        $6,
        $7,
        $8,
        $9,
        $10
    )
    ON CONFLICT (endpoint_id, company_id, created_at) DO UPDATE 
    set
        created_at = $11,
        rec = $12,
        ent = $13,
        age = $14,
        fwd = $15,
        total_lost_messages = $16,
        total_timeout_messages = $17,
        counted_items_on_fwd_queues = $18,
        company_id = $19,
        endpoint_id = $20
    `
	fmt.Printf("forwarder.database.WriteQueueCheckpoint() Result: created:%v rec:%v ent:%v age:%v fwd:%v lost:%v timeout:%v qsize:%v company:%v endpoint:%v\n", created_at, rec, ent, age, fwd, total_lost_messages, total_timeout_messages, queueSize, companyId, endpointId)
	_, err = dbconn.Exec(context.Background(), q,
		created_at, rec, ent, age, fwd, total_lost_messages, total_timeout_messages, queueSize, companyId, endpointId,
		created_at, rec, ent, age, fwd, total_lost_messages, total_timeout_messages, queueSize, companyId, endpointId)
	if err != nil {
		return err
	}

	return nil
}

func CalculateQueueSize(endpointId int) (int, error) {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return -1, err
	}

	var created_at time.Time
	var rec int
	var ent int
	var age int
	var fwd int
	var total_lost_messages int
	var total_timeout_messages int
	var counted_items_on_fwd_queues int
	var companyId int
	// The 7 days is just because I wanted some kind of limit
	q := `
    select
        created_at,
        0 - rec,
        0 - ent,
        0 - age,
        0 - fwd,
        0 - total_lost_messages,
        0 - total_timeout_messages,
        counted_items_on_fwd_queues,
        company_id
    from
        webhook_queue_size_checkpoint_v2
    where
        endpoint_id = $1 and
        created_at > now() - interval '7 days'
    order by
        created_at desc
    limit 1
    `
	err = dbconn.QueryRow(context.Background(), q, endpointId).Scan(&created_at, &rec, &ent, &age, &fwd, &total_lost_messages, &total_timeout_messages, &counted_items_on_fwd_queues, &companyId)
	if err != nil {
		// TODO what's the right way to check if no rows OR query failed?
		if ! strings.Contains(err.Error(), "no rows in ") {
			return 0, err
		}

		fmt.Printf("forwarder.database.CalculateQueueSize(): no checpoint row, assuming zeros\n")
		return 0, nil
	}

	var day_rec int
	var day_age int
	var day_fwd int
	var day_ent int
	var day_total_lost_messages int
	var day_total_timeout_messages int
	q = `
    select
        rec_h00 + rec_h01 + rec_h02 + rec_h03 + rec_h04 + rec_h05 + rec_h06 + rec_h07 + rec_h08 + rec_h09 + rec_h10 + rec_h11 + rec_h12 + rec_h13 + rec_h14 + rec_h15 + rec_h16 + rec_h17 + rec_h18 + rec_h19 + rec_h20 + rec_h21 + rec_h22 + rec_h23 as sum_rec,
	    coalesce(age_h00,0) + coalesce(age_h01,0) + coalesce(age_h02,0) + coalesce(age_h03,0) + coalesce(age_h04,0) + coalesce(age_h05,0) + coalesce(age_h06,0) + coalesce(age_h07,0) + coalesce(age_h08,0) + coalesce(age_h09,0) + coalesce(age_h10,0) + coalesce(age_h11,0) + coalesce(age_h12,0) + coalesce(age_h16,0) + coalesce(age_h20,0) + coalesce(age_h24,0) + coalesce(age_h28,0) + coalesce(age_h32,0) + coalesce(age_h36,0) + coalesce(age_h42,0) + coalesce(age_h48,0) + coalesce(age_h54,0) + coalesce(age_h60,0) + coalesce(age_h66,0) as sum_age,
        fwd_h00 + fwd_h01 + fwd_h02 + fwd_h03 + fwd_h04 + fwd_h05 + fwd_h06 + fwd_h07 + fwd_h08 + fwd_h09 + fwd_h10 + fwd_h11 + fwd_h12 + fwd_h13 + fwd_h14 + fwd_h15 + fwd_h16 + fwd_h17 + fwd_h18 + fwd_h19 + fwd_h20 + fwd_h21 + fwd_h22 + fwd_h23 as sum_fwd,
        ent_h00 + ent_h01 + ent_h02 + ent_h03 + ent_h04 + ent_h05 + ent_h06 + ent_h07 + ent_h08 + ent_h09 + ent_h10 + ent_h11 + ent_h12 + ent_h13 + ent_h14 + ent_h15 + ent_h16 + ent_h17 + ent_h18 + ent_h19 + ent_h20 + ent_h21 + ent_h22 + ent_h23 as sum_ent,
        total_lost_messages as total_lost_messages,
        total_timeout_messages as total_timeout_messages
    from 
        webhook_forwarder_daily_forward_stats_v3
    where
        endpoint_id = $1 AND
        company_id = $2 AND
        created_at >= $4
    `
	rows, err := dbconn.Query(context.Background(), q, endpointId, companyId, created_at)
	if err != nil {
		return -1, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&day_rec, &day_age, &day_fwd, &day_ent, &day_total_lost_messages, &day_total_timeout_messages)
		if nil != err {
			return -1, err
		}

		rec += day_rec
		ent += day_ent
		age += day_age
		fwd += day_fwd
		total_lost_messages += day_total_lost_messages
		total_timeout_messages += day_total_timeout_messages
	}

	result := counted_items_on_fwd_queues + ent - fwd - total_lost_messages - total_timeout_messages
	// fmt.Printf("a %v + %v - %v - %v - %v = %v \n", counted_items_on_fwd_queues, ent, fwd, total_lost_messages, total_timeout_messages, result)

	t := time.Now().UTC()
	hourNow := t.Hour()
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)

	q = `
    update webhook_forwarder_daily_forward_stats_v3
    set
        ` + fmt.Sprintf("qus_h%02d", hourNow) + ` = $1
    where
        company_id = $2 AND
        created_at = $3
    `
	_, err = dbconn.Exec(context.Background(), q, result, companyId, day)
	if err != nil {
		return result, err
	}

	return result, nil
}

func GetLatestActiveCompanies() ([]int, error) {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return nil, err
	}

	var companies = make([]int, 0, 100000)

	q := `
    select
        distinct(company_id)
    from
        webhook_forwarder_daily_forward_stats_v3
    where
        created_at > now() - interval '3 day'
    `
	rows, err := dbconn.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var companyId int
	for rows.Next() {
		err = rows.Scan(&companyId)
		companies = append(companies, companyId)
	}

	return companies, nil
}

type EndpointCompany struct {
	EndPointId          int
	CompanyId           int
}

func GetLatestActiveEndpoints() ([]EndpointCompany, error) {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return nil, err
	}

	var companies = make([]EndpointCompany, 0, 100000)

	q := `
    select
        distinct endpoint_id, company_id
    from
        webhook_forwarder_daily_forward_stats_v3
    where
        created_at > now() - interval '3 day'
    `
	rows, err := dbconn.Query(context.Background(), q)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	var endpointId int
	var companyId int
	for rows.Next() {
		err = rows.Scan(&endpointId, &companyId)
		companies = append(companies, EndpointCompany{EndPointId: endpointId, CompanyId: companyId})
	}

	return companies, nil
}

func CalculateQueueSizes() error {

	companies, err := GetLatestActiveCompanies()
	if nil != err {
		return err
	}

    for _, companyId := range companies {
		qsize, err := CalculateQueueSize(companyId)
		if err != nil {
			return err
		}
		fmt.Printf("forwarder.pg.CalculateQueueSizes() companyId:%d queueSize:%d\n", companyId, qsize)
	}

	return nil
}

type CompanyQueueSize struct {
	CompanyId     int
	PresentHour   int
	QueueSize     int
	WarnedAt      *time.Time  // nullable!
	DisabledAt    *time.Time  // nullable!
	AlertEmail    string
	EndpointId    int
}

func GetCompaniesAndQueueSizes() ([]*CompanyQueueSize, error) {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return nil, err
	}

	now := time.Now().UTC()
	hourNow := now.Hour()
	day := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)

	outArray := make([]*CompanyQueueSize, 0, 0)

	var companyId int
	var presentHour = hourNow
	var queueSize int
	var warnedAt pq.NullTime
	var disabledAt pq.NullTime
	var alertEmail string
	var endpointId int

	// TODO add some min level. Eg. only bother if >1000 items in queue
	q := `
    select
        ws.company_id,
        ws.qus_h` + fmt.Sprintf("%02d", hourNow) + `,
        wc.warned_at,
        wc.disabled_at,
        uc.alert_email,
        ws.endpoint_id
    from
        webhook_forwarder_daily_forward_stats_v3 ws,
        webhook_forwarder_poll_cfg wc,
        users_company uc
    where
        ws.created_at = $1 AND
        ws.company_id = wc.company_id AND
        wc.disabled_at is null AND
        uc.id = ws.company_id
    `
	rows, err := dbconn.Query(context.Background(), q, day)
	if err != nil {
		return nil, err
	}

	defer rows.Close()
	for rows.Next() {
		err = rows.Scan(&companyId, &queueSize, &warnedAt, &disabledAt, &alertEmail, &endpointId)
		if nil != err {
			return nil, err
		}

		cqs := CompanyQueueSize{
			CompanyId: companyId,
			PresentHour: presentHour,
			QueueSize: queueSize,
			AlertEmail: alertEmail,
			EndpointId: endpointId,
		}

		if warnedAt.Valid {
			cqs.WarnedAt = &warnedAt.Time
		}

		if disabledAt.Valid {
			cqs.DisabledAt = &disabledAt.Time
		}

		outArray = append(outArray, &cqs)
	}

	return outArray, nil
}

func SetWarnedAt(companyId int) error {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return err
	}

	q := `
    update webhook_forwarder_poll_cfg
    set
        warned_at = now()
    where
        company_id = $1
    `
	_, err = dbconn.Exec(context.Background(), q, companyId)
	if nil != err {
		return err
	}

	return nil
}

// DisableCompany sets the disabled_at and falsifies all rows with forwarder data. You need to mail the customer elsewhere.
func DisableCompany(companyId int) error {
	dbUsageMutex.Lock()
	defer dbUsageMutex.Unlock()

	dbconn, err := GetDbConnection()
	if nil != err {
		return err
	}

	q := `
    update webhook_forwarder_poll_cfg
    set
        disabled_at = now()
    where
        company_id = $1
    `
	_, err = dbconn.Exec(context.Background(), q, companyId)
	if nil != err {
		return fmt.Errorf("forwarder.database.pg.DisableCompany() failed to set disabled_at: %v", err)
	}

	q = `
    update webhook_forwarder_poll_endpoint
    set
        is_active = false
    where
        company_id = $1 and
        is_active = true
    `
	_, err = dbconn.Exec(context.Background(), q, companyId)
	if nil != err {
		return fmt.Errorf("forwarder.database.pg.DisableCompany() failed to update is_active: %v", err)
	}

	return nil
}
