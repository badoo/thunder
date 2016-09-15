package common

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	"badoo/_packages/service/config"
	"github.com/badoo/thunder/proto"
	"database/sql"
)

type (
	FullConfig struct {
		badoo_config.ServiceConfig
		thunder.ConfigT
	}

	Jobs struct {
		Type             string
		Min              int
		Max              int
		Have_finish_jobs bool
		Temporary        bool
	}

	NextTsCallback struct {
		Callback string
		Settings map[string]string
	}

	ScriptSettings struct {
		id                    uint64
		class_name            string
		instance_count        int
		max_time              int
		jobs                  Jobs
		have_next_ts_callback bool
		next_ts_callback      NextTsCallback
		repeat                uint32
		retry                 sql.NullInt64
		ttl                   uint32
		repeat_job            sql.NullInt64
		retry_job             uint32
		location              string
		location_type         string
		developer             sql.NullString
		max_retries           uint32
		profiling_enabled     int
		created               int64
	}
)

var (
	// TODO: implement setting of this value
	exitWithParent bool
	startDelaySec  int
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type UInt64Slice []uint64

func (p UInt64Slice) Len() int           { return len(p) }
func (p UInt64Slice) Less(i, j int) bool { return p[i] < p[j] }
func (p UInt64Slice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func Setup() {
	if exitWithParent {
		go func() {
			buf := make([]byte, 10)
			for {
				_, err := os.Stdin.Read(buf)
				if err != nil {
					fmt.Println("Cannot read from stdin (" + err.Error() + "), exiting")
					os.Exit(0)
				}
			}
		}()
	}

	if startDelaySec > 0 {
		time.Sleep(time.Duration(startDelaySec) * time.Second)
	}
}
