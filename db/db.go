package db

import (
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"

	"badoo/_packages/log"

	"strconv"

	_ "github.com/go-sql-driver/mysql"
)

type (
	LazyTrx struct {
		level           uint
		tx              *sql.Tx
		commitCallbacks []func()
		Name            string
	}

	RawData struct {
		Data string
	}
)

const (
	TABLE_STATUS                    = "Status"
	TABLE_LOCKS                     = "Locks"
	TABLE_RESOURCE_SHORTAGE_LOG     = "ResourceShortageLog"
	TABLE_RESOURCE_SHORTAGE_LOG_OLD = "ResourceShortageLog_old"
)

//language=SQL
const (
	QUERY_LOG_CYCLE_START = `INSERT INTO ` + TABLE_STATUS + ` (class_name, instance_idx, hostname, cycle_start_ts)
        VALUES ('#class_name#', #instance_idx#, '#hostname#', NOW())
        ON DUPLICATE KEY UPDATE hostname = '#hostname#', cycle_start_ts = NOW()`

	QUERY_LOG_CYCLE_STOP = `UPDATE ` + TABLE_STATUS + ` SET cycle_stop_ts = NOW(), success = #success#
        WHERE class_name = '#class_name#' AND instance_idx = #instance_idx# AND hostname = '#hostname#'`

	QUERY_SET_LOCK = `UPDATE ` + TABLE_LOCKS + ` SET hostname = '#hostname#', updated = NOW(), updated_ns = #nsec#
		WHERE name = '#lock_name#' AND (hostname = '#hostname#' OR updated < NOW() - INTERVAL 1 MINUTE)`
)

var (
	db     *sql.DB
	dbName string
)

func GetDbName() string {
	return dbName
}

// grabbed from mymysql examples
func EscapeString(txt string) string {
	var (
		esc string
		buf bytes.Buffer
	)
	last := 0
	for ii, bb := range txt {
		switch bb {
		case 0:
			esc = `\0`
		case '\n':
			esc = `\n`
		case '\r':
			esc = `\r`
		case '\\':
			esc = `\\`
		case '\'':
			esc = `\'`
		case '"':
			esc = `\"`
		case '\032':
			esc = `\Z`
		default:
			continue
		}
		io.WriteString(&buf, txt[last:ii])
		io.WriteString(&buf, esc)
		last = ii + 1
	}
	io.WriteString(&buf, txt[last:])
	return buf.String()
}

func Setup(dsn string, dbMaxConns uint32) error {
	var err error

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		return err
	}

	db.SetMaxOpenConns(int(dbMaxConns))

	parts := strings.Split(dsn, ")")
	if len(parts) != 2 {
		log.Fatalf("Unexpected MySQL DSN format, expected it to in form '<login>:<password>@<transport>(...)/<db>'")
	}

	parts = strings.Split(parts[1], "/")
	if len(parts) != 2 {
		log.Fatalf("Unexpected MySQL DSN format, expected it to be in form of '<host_settings>/<db>'")
	}

	qParts := strings.Split(parts[1], "?")
	dbName = qParts[0]

	return nil
}

func DoInLazyTransaction(cb func(tx *LazyTrx) error) error {
	return DoInLazyTransactionWithTrx(cb, new(LazyTrx))
}

func DoInLazyTransactionWithTrx(cb func(tx *LazyTrx) error, trx *LazyTrx) (err error) {
	if err = trx.Begin(); err != nil {
		return err
	}

	func() {
		// prevent from trx leakage upon cb panic
		defer func() {
			if r := recover(); r != nil {
				trx.Rollback()
				panic(r)
			}
		}()

		err = cb(trx)
	}()

	if err != nil {
		trx.Rollback()
		return
	}

	err = trx.Commit()
	return
}

func (p *LazyTrx) AddCommitCallback(cb func()) {
	p.commitCallbacks = append(p.commitCallbacks, cb)
}

func (p *LazyTrx) Begin() error {
	p.level++
	return nil
}

func (p *LazyTrx) Commit() error {
	if p.level == 0 {
		buf := make([]byte, 5000)
		n := runtime.Stack(buf, false)
		log.Warning("Requested commit for zero level: " + string(buf[0:n]))
	}

	p.level--

	if p.level == 0 && p.tx != nil {
		err := p.tx.Commit()
		DebugPrintln(p.Name, "COMMIT")
		if err != nil {
			return err
		}

		if p.commitCallbacks != nil {
			for _, cb := range p.commitCallbacks {
				cb()
			}
		}

		p.tx = nil
	}

	return nil
}

func (p *LazyTrx) Rollback() error {
	if p.tx != nil {
		err := p.tx.Rollback()
		DebugPrintln(p.Name, "ROLLBACK")
		if err != nil {
			return err
		}
	}

	p.level = 0
	p.tx = nil

	return nil
}

func DebugPrintln(localPrefix, str string) {
	log.Debugln(localPrefix, "\n", str)
}

func (p *LazyTrx) prepareFirstQuery(queryTpl string, args ...interface{}) (query string, err error) {
	if p.tx == nil && p.level > 0 {
		p.tx, err = db.Begin()
		DebugPrintln(p.Name, "BEGIN")
		if err != nil {
			return "", err
		}
	}

	query = TplQuery(queryTpl, args...)
	DebugPrintln(p.Name, query)

	return query, nil
}

func (p *LazyTrx) Query(queryTpl string, args ...interface{}) (*sql.Rows, error) {
	q, err := p.prepareFirstQuery(queryTpl, args...)
	if err != nil {
		return nil, err
	}

	return p.tx.Query(q)
}

func (p *LazyTrx) Exec(queryTpl string, args ...interface{}) (sql.Result, error) {
	q, err := p.prepareFirstQuery(queryTpl, args...)
	if err != nil {
		return nil, err
	}

	res, err := p.tx.Exec(q)

	if err != nil {
		trace := make([]byte, 8192)
		n := runtime.Stack(trace, false)
		log.Warningf("Failed SQL query:\n'%s',\n\nReason: '%s',\n\nStack trace: %s\n", q, err.Error(), trace[0:n])
	}

	return res, err
}

func INStr(args []string) *RawData {
	values := make([]string, 0, len(args))
	for _, arg := range args {
		values = append(values, "'"+EscapeString(arg)+"'")
	}

	return &RawData{Data: strings.Join(values, ", ")}
}

func INUint64(args []uint64) *RawData {
	values := make([]string, 0, len(args))

	for _, val := range args {
		values = append(values, strconv.FormatUint(val, 10))
	}

	return &RawData{Data: strings.Join(values, ",")}
}

func QNullInt64(arg sql.NullInt64) *RawData {
	if !arg.Valid {
		return &RawData{Data: "NULL"}
	}

	return &RawData{Data: strconv.FormatInt(arg.Int64, 10)}
}

func TplQuery(query string, args ...interface{}) string {
	for i := 0; i < len(args); i += 2 {
		secondArg := args[i+1]
		var secondArgStr string

		if raw, ok := secondArg.(*RawData); ok {
			secondArgStr = raw.Data
		} else {
			secondArgStr = EscapeString(fmt.Sprint(secondArg))
		}

		query = strings.Replace(query, "#"+args[i].(string)+"#", secondArgStr, -1)
	}

	return query
}

func Query(queryTpl string, args ...interface{}) (rows *sql.Rows, err error) {
	query := TplQuery(queryTpl, args...)

	DebugPrintln("", query)

	return db.Query(query)
}

func Exec(queryTpl string, args ...interface{}) (sql.Result, error) {
	query := TplQuery(queryTpl, args...)

	DebugPrintln("", query)

	return db.Exec(query)
}

func LockCycle(lockName, hostname string) (bool, error) {
	res, err := Exec(
		QUERY_SET_LOCK,
		"lock_name", lockName,
		"hostname", hostname,
		"nsec", time.Now().Nanosecond())

	if err != nil {
		return false, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}

	return affected > 0, nil
}

func LogCycleStart(className, hostname string, instanceIdx int) error {
	_, err := Exec(QUERY_LOG_CYCLE_START, "class_name", className, "hostname", hostname, "instance_idx", instanceIdx)
	return err
}

func LogCycleStop(className, hostname string, instanceIdx int, success int) error {
	_, err := Exec(
		QUERY_LOG_CYCLE_STOP,
		"class_name", className,
		"hostname", hostname,
		"instance_idx", instanceIdx,
		"success", success)

	return err
}
