package jobgen

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"github.com/badoo/thunder/common"
	"github.com/badoo/thunder/db"
	"github.com/badoo/thunder/proto/phproxyd"

	"github.com/gogo/protobuf/proto"
)

type LauncherState struct {
	Waiting  []*RunQueueEntry
	Init     []*RunQueueEntry
	Running  []*RunQueueEntry
	Finished []*RunQueueEntry

	RawResp string
}

type LauncherDebugPrintRequest struct {
	RespCh chan *LauncherState
}

type LauncherUpdateStatusRequest struct {
	Hostname   string
	RunId      uint64
	PrevStatus string
	Status     string
	errCh      chan error
}

type LauncherLogFinishResponse struct {
	err  error
	info *FinishResultRunInfo
}

type LauncherLogFinishRequest struct {
	Hostname   string
	RunId      uint64
	PrevStatus string
	Success    bool
	respCh     chan *LauncherLogFinishResponse
}

type LauncherData struct {
	// setup params
	hostname              string
	waitingCh             chan *RunQueueEntry
	allRowsCh             chan []*RunQueueEntry // all rq entries are periodically selected from runqueue to detect externally added rows
	debugCh               chan *LauncherDebugPrintRequest
	updateStatusRequestCh chan *LauncherUpdateStatusRequest
	logFinishRequestCh    chan *LauncherLogFinishRequest

	// state
	waitingMap  map[uint64]*RunQueueEntry
	initMap     map[uint64]*RunQueueEntry
	runningMap  map[uint64]*RunQueueEntry
	finishedMap map[uint64]*RunQueueEntry

	allMap map[uint64]*RunQueueEntry

	killedRecently map[uint64]bool

	deletedIds map[uint64]int   // run_id => refcount
	tickCh     <-chan time.Time // update status of init, running and finished tasks
}

var statusPriority = map[string]int{
	RUN_STATUS_WAITING:  0,
	RUN_STATUS_INIT:     1,
	RUN_STATUS_RUNNING:  2,
	RUN_STATUS_FINISHED: 3,
}

var hostSuffix string
var basePath string
var haveDeveloper bool
var developerPath string

const LAUNCHER_DB_DEBUG = false

func copyRunQueueMap(m map[uint64]*RunQueueEntry) []*RunQueueEntry {
	res := make([]*RunQueueEntry, 0, len(m))
	for _, origV := range m {
		v := new(RunQueueEntry)
		*v = *origV
		res = append(res, v)
	}
	return res
}

func (d *LauncherData) call(req proto.Message) (msg_id uint32, resp proto.Message, err error) {
	client := gpbrpc.NewClient(d.hostname+hostSuffix, &badoo_phproxyd.Gpbrpc, &gpbrpc.GpbsCodec, time.Second, time.Second)
	defer client.Close()
	msg_id, resp, err = client.Call(req)
	if err != nil {
		log.Warningf("Call failed for host %s for message %+v, got error: %s", d.hostname, req, err.Error())
	}
	return
}

func (d *LauncherData) acceptWaiting(jobs []*RunQueueEntry) {
	idsToSelect := make([]uint64, 0)
	settingsMap := make(map[uint64]*ScriptSettings)

	for _, row := range jobs {
		if row.settings == nil {
			log.Warningf("Incorrect row in run queue (waitingCh), settings are invalid: %+v", row)
			continue
		}

		if d.allMap[row.Id] != nil {
			continue
		}
		settingsMap[row.Id] = row.settings
		idsToSelect = append(idsToSelect, row.Id)
	}

	rqs, err := getRunningInfos(idsToSelect)
	if err != nil {
		log.Warnf("acceptWaiting could not select run_queue entries: %s", err.Error())
		return
	}
	for _, row := range rqs {
		row.settings = settingsMap[row.Id]
		d.addToMaps(row)
	}

	d.processWaiting()
}

func (d *LauncherData) processDebugEvent(ev *LauncherDebugPrintRequest) {
	resp := new(LauncherState)
	var b bytes.Buffer

	fmt.Fprintf(&b, "Asked to print state of launcher goroutine hostname=%s\n", d.hostname)

	fmt.Fprintf(&b, "\nWaiting:\n")
	resp.Waiting = copyRunQueueMap(d.waitingMap)
	for tt_id, row := range d.waitingMap {
		fmt.Fprintf(&b, "  #%d: %+v\n", tt_id, row)
	}

	fmt.Fprintf(&b, "\nInit:\n")
	resp.Init = copyRunQueueMap(d.initMap)
	for tt_id, row := range d.initMap {
		fmt.Fprintf(&b, "  #%d: %+v\n", tt_id, row)
	}

	fmt.Fprintf(&b, "\nRunning:\n")
	resp.Running = copyRunQueueMap(d.runningMap)
	for tt_id, row := range d.runningMap {
		fmt.Fprintf(&b, "  #%d: %+v\n", tt_id, row)
	}

	fmt.Fprintf(&b, "\nFinished:\n")
	resp.Finished = copyRunQueueMap(d.finishedMap)
	for tt_id, row := range d.finishedMap {
		fmt.Fprintf(&b, "  #%d: %+v\n", tt_id, row)
	}

	fmt.Fprintf(&b, "\n\nDeleted ids: %+v\n", d.deletedIds)

	resp.RawResp = b.String()

	select {
	case ev.RespCh <- resp:
	default:
	}
}

func (d *LauncherData) mainThread() {
	d.init()
	i := 0

	for {
		select {
		case ev := <-d.debugCh:
			d.processDebugEvent(ev)

		case jobsRaw := <-d.waitingCh:
			l := len(d.waitingCh)
			megaJobs := make([]*RunQueueEntry, 0, l+1)
			megaJobs = append(megaJobs, jobsRaw)
			for i := 0; i < l; i++ {
				megaJobs = append(megaJobs, <-d.waitingCh)
			}
			d.acceptWaiting(megaJobs)

		case rows := <-d.allRowsCh:
			d.processAllRows(rows)

		case req := <-d.updateStatusRequestCh:
			d.processUpdateStatusRequest(req)

		case req := <-d.logFinishRequestCh:
			d.processLogFinishRequest(req)

		case <-d.tickCh:
			d.cycle()

			i++
			if i%10 == 0 {
				d.killedRecently = make(map[uint64]bool)
			}
		}
	}
}

func (d *LauncherData) cycle() {
	d.tickCh = time.After(time.Second)

	d.processFinished()
	d.processWaitingInInit()
	d.processWaiting()
	d.processRunningTooLong()
}

const (
	PHPROXY_TAG      = "scriptframework"
	INIT_TIMEOUT_SEC = 20

	DEVELOPER_CUSTOM_PATH_TIMEOUT = 14400 // how long "developer" field has effect, in seconds
	PROFILING_TIMEOUT             = 3600  // how long "profiling_enabled" field has effect, in seconds
	DEBUG_TIMEOUT                 = 1200  // how long "debug_enabled" field has effect, in seconds

	KILL_ACTION_NO_ACTION                 = "noAction"
	KILL_ACTION_DELETE_FROM_QUEUE         = "deleteFromQueue"
	KILL_ACTION_SET_WAITING               = "setWaiting"
	KILL_ACTION_LOG_SCRIPT_FINISH_INIT    = "logScriptFinishInit"
	KILL_ACTION_LOG_SCRIPT_FINISH_RUNNING = "logScriptFinishRunning"
)

func getScriptPath(settings *ScriptSettings) string {
	if haveDeveloper && settings.developer.Valid && settings.developer.String != "" {
		if time.Now().Unix()-settings.created < DEVELOPER_CUSTOM_PATH_TIMEOUT {
			return fmt.Sprintf(developerPath, filepath.Base(settings.developer.String))
		}
	}

	return basePath
}

func (d *LauncherData) processWaiting() {
	//	invalidEntries := make([]uint64, 0)
	var rawResp proto.Message

	for run_id, row := range d.waitingMap {
		err := db.DoInLazyTransaction(func(tx *db.LazyTrx) error {
			return setRunStatusToInit(tx, run_id, row.settings.max_time)
		})

		if err != nil {
			log.Errorf("Could not update run status of run_id=%d to %s: %s", run_id, RUN_STATUS_INIT, err.Error())
			return
		}

		// TODO: add host unreachable check

		d.updateStatus(row, RUN_STATUS_INIT)
		row.max_finished_ts.Int64 = row.created.Int64 + int64(row.settings.max_time)
		row.max_finished_ts.Valid = true

		script := getScriptPath(row.settings)

		params := []string{
			fmt.Sprintf("--id=%d", row.Id),
			row.ClassName,
			fmt.Sprintf("--instance-count=%d", row.settings.instance_count),
			fmt.Sprintf("--settings-id=%d", row.settings_id),
			fmt.Sprintf("--method=%s", row.method),
			fmt.Sprintf("--token=%s", row.token),
			fmt.Sprintf("--retry-attempt=%d", row.retry_attempt),
			fmt.Sprintf("--max-retries=%d", row.settings.max_retries),
			fmt.Sprintf("--max-ts=%d", row.created.Int64+int64(row.settings.max_time)),
			fmt.Sprintf("--force-sf-db=%s", db.GetDbName()),
		}

		if row.settings.named_params.Valid && row.settings.named_params.String != "" {
			params = append(params, fmt.Sprintf("--named-params=%s", row.settings.named_params.String))
		}

		if row.JobData != "" {
			params = append(params, fmt.Sprintf("--job-data=%s", row.JobData))
		}

		if testId := os.Getenv("PHPUNIT_SELENIUM_TEST_ID"); testId != "" {
			params = append(params, fmt.Sprintf("--PHPUNIT_SELENIUM_TEST_ID=%s", testId))
		}

		if row.settings.debug_enabled == 1 && row.settings.created > time.Now().Unix()-DEBUG_TIMEOUT {
			params = append(params, "--debug-mode")
		}

		if row.settings.profiling_enabled == 1 && row.settings.created > time.Now().Unix()-PROFILING_TIMEOUT {
			params = append(params, "--enable-profiling")
		}

		if row.timetable_id.Valid && row.timetable_id.Int64 != 0 {
			params = append(params, fmt.Sprintf("--timetable-id=%d", row.timetable_id.Int64))
		}

		ev := &badoo_phproxyd.RequestRun{
			Script:       proto.String(script),
			Hash:         proto.Uint64(row.Id),
			Tag:          proto.String(PHPROXY_TAG),
			Force:        proto.Int32(1),
			Params:       params,
			Store:        badoo_phproxyd.StoreT_FILES.Enum(),
			FreeAfterRun: proto.Bool(false),
		}

		_, rawResp, err = d.call(ev)
		if err != nil {
			continue
		}

		resp, ok := rawResp.(*badoo_phproxyd.ResponseGeneric)
		if !ok {
			log.Errorf("Unexpected response from host %s when doing run, type: %T, response: %+v", d.hostname, rawResp, rawResp)
			continue
		}

		if resp.GetErrorCode() != 0 {
			log.Errorf("Unexpected response from host %s when doing run, got code %d and text %s", d.hostname, resp.GetErrorCode(), resp.GetErrorText())
			continue
		}
	}
}

func (d *LauncherData) processUpdateStatusRequest(req *LauncherUpdateStatusRequest) {
	var err error
	defer func() { req.errCh <- err }()

	el := d.allMap[req.RunId]

	if el == nil {
		err = fmt.Errorf("No such rq row id=%d", req.RunId)
		return
	}

	if el.RunStatus != req.PrevStatus {
		err = fmt.Errorf("Previous status mismatch for rq row id=%d: req.prev=%s, actual=%s", req.RunId, req.PrevStatus, el.RunStatus)
		return
	}

	err = db.DoInLazyTransaction(func(tx *db.LazyTrx) error {
		return updateRunStatus(tx, req.RunId, req.Status, req.PrevStatus)
	})

	if err != nil {
		log.Errorf("Could not update run status of run_id=%d to %s: %s", req.RunId, req.Status, err.Error())
		return
	}

	d.updateStatus(el, req.Status)
}

func (d *LauncherData) processLogFinishRequest(req *LauncherLogFinishRequest) {
	var err error
	var info *FinishResultRunInfo
	defer func() { req.respCh <- &LauncherLogFinishResponse{err: err, info: info} }()

	el := d.allMap[req.RunId]

	if el == nil {
		err = fmt.Errorf("No such rq row id=%d", req.RunId)
		return
	}

	if el.RunStatus != req.PrevStatus {
		err = fmt.Errorf("Previous status mismatch for rq row id=%d: req.prev=%s, actual=%s", req.RunId, req.PrevStatus, el.RunStatus)
		return
	}

	if err = d.persistFinishAndNotify(el, req.Success, req.PrevStatus); err != nil {
		return
	}

	d.updateStatus(el, RUN_STATUS_FINISHED)
	el.finished_ts.Int64 = time.Now().Unix()
	el.finished_ts.Valid = true

	info = d.convertRunQueueEntryToFinishInfo(el)

	d.delFromMaps(req.RunId)
	d.call(&badoo_phproxyd.RequestFree{Hash: proto.Uint64(req.RunId)})
}

func (d *LauncherData) processAllRows(rows []*RunQueueEntry) {
	for _, row := range rows {
		if row.settings == nil {
			log.Warningf("Incorrect row in run queue, settings are invalid: %+v", row)
			continue
		}

		el, ok := d.allMap[row.Id]
		if !ok {
			if d.deletedIds[row.Id] == 0 {
				d.addToMaps(row)
			}
			continue
		}

		if statusPriority[row.RunStatus] > statusPriority[el.RunStatus] || row.RunStatus == RUN_STATUS_WAITING && el.RunStatus == RUN_STATUS_INIT && row.init_attempts > el.init_attempts {
			d.updateStatus(el, row.RunStatus)
			*el = *row
		}

		// external kill request has come
		if row.max_finished_ts.Valid && row.max_finished_ts.Int64 < el.max_finished_ts.Int64 {
			el.max_finished_ts = row.max_finished_ts
			el.stopped_employee_id = row.stopped_employee_id
		}
	}

	for runId, refCount := range d.deletedIds {
		if refCount--; refCount <= 0 {
			delete(d.deletedIds, runId)
		} else {
			d.deletedIds[runId] = refCount
		}
	}
}

func (d *LauncherData) processFinished() {
	var finishedIds []uint64
	var err error

	for run_id := range d.finishedMap {
		d.call(&badoo_phproxyd.RequestFree{Hash: proto.Uint64(run_id)})
		finishedIds = append(finishedIds, run_id)
	}

	if finishedIds == nil || len(finishedIds) == 0 {
		return
	}

	sort.Sort(common.UInt64Slice(finishedIds))

	err = db.DoInLazyTransaction(func(tx *db.LazyTrx) error {
		return deleteFromRunQueue(tx, finishedIds, RUN_STATUS_FINISHED)
	})

	if err != nil {
		log.Errorf("Could not delete rows from run queue for hostname=%s: %s", d.hostname, err.Error())
		return
	}

	for _, v := range d.finishedMap {
		d.delFromMaps(v.Id)
	}
}

const MAX_TIME_DISCREPANCY = 3

// We set alarm on the end host so that we can be sure that script is dead after created + max_time
func shouldHaveFinished(row *RunQueueEntry) bool {
	return time.Now().Unix()-MAX_TIME_DISCREPANCY-int64(row.settings.max_time) > row.created.Int64
}

func (d *LauncherData) delFromMaps(id uint64) {
	row := d.allMap[id]
	if row == nil {
		log.Warnf("Trying to delete nonexistent from run_queue: %+v", row)
		return
	}
	if m := d.getMapByStatus(row.RunStatus); m != nil {
		if m[id] == nil {
			log.Warnf("Run queue entry missing in its map when deleting: %+v", row)
			return
		}
		delete(m, id)
	} else {
		log.Warnf("Broken run status when deleting: %+v", row)
		return
	}
	delete(d.allMap, id)
	d.deletedIds[id] = DELETE_IDS_KEEP_GENERATIONS
}

func (d *LauncherData) addToMaps(row *RunQueueEntry) {
	if row.settings == nil {
		buf := make([]byte, 5000)
		n := runtime.Stack(buf, false)
		log.Warningf("Incorrect row in run queue (addToMaps), settings are invalid: %+v\ntrace:%s", row, buf[0:n])
		return
	}
	if m := d.getMapByStatus(row.RunStatus); m != nil {
		if LAUNCHER_DB_DEBUG {
			log.Printf("RQ row from db: id=%d, class=%s, job_data=%s, hostname=%s", row.Id, row.ClassName, row.JobData, row.hostname)
		}
		if d.allMap[row.Id] != nil {
			log.Warnf("Trying to add already added into run_queue (all map): %+v", row)
			return
		}
		if m[row.Id] != nil {
			log.Warnf("Trying to add already added into run_queue (own map): %+v", row)
			return
		}
		m[row.Id] = row
		d.allMap[row.Id] = row
	} else {
		log.Warnf("Broken run status: %+v", row)
	}
}

func (d *LauncherData) getMapByStatus(status string) map[uint64]*RunQueueEntry {
	switch status {
	case RUN_STATUS_WAITING:
		return d.waitingMap
	case RUN_STATUS_INIT:
		return d.initMap
	case RUN_STATUS_RUNNING:
		return d.runningMap
	case RUN_STATUS_FINISHED:
		return d.finishedMap
	}
	return nil
}

func (d *LauncherData) updateStatus(row *RunQueueEntry, newStatus string) {
	if row.RunStatus == newStatus {
		return
	}

	if LAUNCHER_DB_DEBUG {
		log.Printf("Updating status of row #%d (%s) from %s to %s", row.Id, row.ClassName, row.RunStatus, newStatus)
	}

	if old := d.getMapByStatus(row.RunStatus); old != nil {
		delete(old, row.Id)
	}

	if m := d.getMapByStatus(newStatus); m != nil {
		m[row.Id] = row
	}

	row.RunStatus = newStatus

	now := time.Now().Unix()

	if newStatus == RUN_STATUS_WAITING {
		row.waiting_ts.Valid = true
		row.waiting_ts.Int64 = now
	} else if newStatus == RUN_STATUS_INIT {
		row.init_ts.Valid = true
		row.init_ts.Int64 = now
	} else if newStatus == RUN_STATUS_RUNNING {
		row.running_ts.Valid = true
		row.running_ts.Int64 = now
	} else if newStatus == RUN_STATUS_FINISHED {
		row.running_ts.Valid = true
		row.running_ts.Int64 = now
	}
}

func convertTsIntToString(ts sql.NullInt64) interface{} {
	if !ts.Valid {
		return nil
	}

	return time.Unix(ts.Int64, 0).Format("2006-01-02 15:04:05")
}

func (d *LauncherData) persistFinishAndNotify(row *RunQueueEntry, success bool, prevStatus string) error {
	location, err := getLocationIdx(row.settings.location_type, d.hostname)
	if err != nil {
		log.Warningf("Could not get location idx for row %+v, settings: %+v, reason: %s", row, row.settings, err.Error())
		return err
	}

	if row.timetable_id.Int64 == 0 {
		err := db.DoInLazyTransaction(func(tx *db.LazyTrx) error {
			return deleteFromRunQueue(tx, []uint64{row.Id}, prevStatus)
		})

		if err != nil {
			log.Warningf("Could not delete incorrectly finished run queue entry in %+v: %s", row, err.Error())
			return err
		}
	} else {
		if err := notifyTTFinished(row.ClassName, location, uint64(row.timetable_id.Int64), row.Id, success, true, prevStatus); err != nil {
			log.Warningf("Could not notify about timetable finish: %s", err.Error())
			return err
		}
	}

	return nil
}

func (d *LauncherData) convertRunQueueEntryToFinishInfo(row *RunQueueEntry) *FinishResultRunInfo {
	if row == nil {
		return nil
	}

	return &FinishResultRunInfo{
		Id:                row.Id,
		TimetableId:       uint64(row.timetable_id.Int64),
		GenerationId:      uint64(row.generation_id.Int64),
		Hostname:          d.hostname,
		HostnameIdx:       row.hostname_idx,
		ClassName:         row.ClassName,
		JobData:           row.JobData,
		Method:            row.method,
		RunStatus:         row.RunStatus,
		Created:           convertTsIntToString(row.created),
		WaitingTs:         convertTsIntToString(row.waiting_ts),
		ShouldInitTs:      convertTsIntToString(row.should_init_ts),
		InitAttempts:      row.init_attempts,
		InitTs:            convertTsIntToString(row.init_ts),
		RunningTs:         convertTsIntToString(row.running_ts),
		MaxFinishedTs:     convertTsIntToString(row.max_finished_ts),
		FinishedTs:        convertTsIntToString(row.finished_ts),
		StoppedEmployeeId: row.stopped_employee_id.Int64,
		Token:             row.token,
		RetryAttempt:      row.retry_attempt,
		SettingsId:        row.settings_id,
	}
}

func (d *LauncherData) logIncorrectFinish(resp *badoo_phproxyd.ResponseCheck, row *RunQueueEntry, prevStatus string) {
	d.call(&badoo_phproxyd.RequestFree{Hash: proto.Uint64(row.Id)})

	if row.RunStatus == RUN_STATUS_FINISHED {
		return
	}

	err := d.persistFinishAndNotify(row, false, prevStatus)
	if err != nil {
		return
	}

	userTime := float64(-1)
	sysTime := float64(-1)
	realTime := float64(-1)
	maxMemory := uint64(1)

	if resp != nil {
		userTime = float64(resp.GetUtimeTvSec()) + float64(resp.GetUtimeTvUsec())/1e6
		sysTime = float64(resp.GetStimeTvSec()) + float64(resp.GetStimeTvUsec())/1e6
		realTime = float64(time.Now().Unix())
		if row.running_ts.Valid {
			realTime -= float64(row.running_ts.Int64)
		} else if row.init_ts.Valid {
			realTime -= float64(row.init_ts.Int64)
		} else {
			realTime -= float64(row.created.Int64)
		}
		maxMemory = uint64(resp.GetMaxrss() * 1024)
	}

	rqhLog <- &FinishResult{
		Id:          row.Id,
		TimetableId: uint64(row.timetable_id.Int64),
		ClassName:   row.ClassName,
		Hostname:    d.hostname,
		Success:     false,
		PrevStatus:  prevStatus,
		Rusage: FinishResultRusage{
			UserTime:  userTime,
			SysTime:   sysTime,
			RealTime:  realTime,
			MaxMemory: maxMemory,
		},
		ProfilingUrl: "",
		Initial:      false,
		Timestamp:    uint64(time.Now().Unix()),
		RunInfo:      *d.convertRunQueueEntryToFinishInfo(row),
	}

	d.delFromMaps(row.Id)
}

func (d *LauncherData) processWaitingInInit() {
	for run_id, cachedRow := range d.initMap {
		_, rawResp, err := d.call(&badoo_phproxyd.RequestCheck{Hash: proto.Uint64(run_id)})

		if shouldHaveFinished(cachedRow) {
			d.logIncorrectFinish(nil, cachedRow, RUN_STATUS_INIT)
			continue
		}

		if err != nil {
			continue
		}

		row, err := getRunningInfo(run_id)
		if err != nil {
			log.Warnf("Could not get running info from DB in method processWaitingInInit for hostname=%s, class=%s, run id=%d, err: %s", d.hostname, cachedRow.ClassName, run_id, err.Error())
			continue
		}

		if cachedRow.settings_id != row.settings_id {
			log.Warnf("Broken row in cache or db for id=%d, settings_id is different (cache=%d, db=%d)", run_id, cachedRow.settings_id, row.settings_id)
			continue
		}

		row.settings = cachedRow.settings

		switch resp := rawResp.(type) {
		case *badoo_phproxyd.ResponseCheck:
			d.logIncorrectFinish(resp, row, row.RunStatus)
		case *badoo_phproxyd.ResponseGeneric:
			result := -badoo_phproxyd.Errno(resp.GetErrorCode())

			if result == badoo_phproxyd.Errno_ERRNO_NOT_FOUND || result == badoo_phproxyd.Errno_ERRNO_ALREADY_RUNNING {
				// we should log intelligently when server is so slow that it does not launch script in 0.5 sec
				if time.Now().Unix()-INIT_TIMEOUT_SEC > row.should_init_ts.Int64 {
					action := KILL_ACTION_NO_ACTION
					if result == badoo_phproxyd.Errno_ERRNO_NOT_FOUND {
						action = KILL_ACTION_LOG_SCRIPT_FINISH_INIT
					}

					d.terminate(row, action)
				} else if result == badoo_phproxyd.Errno_ERRNO_NOT_FOUND {
					d.terminate(row, KILL_ACTION_SET_WAITING)
				}
			} else if result == badoo_phproxyd.Errno_ERRNO_FAILED_FINISHED {
				log.Warningf("Script %s finished with failure at %s", row.ClassName, d.hostname)
				d.logIncorrectFinish(nil, row, RUN_STATUS_INIT)
			} else if result == badoo_phproxyd.Errno_ERRNO_WAIT_FOR_FREE {
				log.Warningf("Waiting in init: Lost results for %s at %s", row.ClassName, d.hostname)
				d.logIncorrectFinish(nil, row, RUN_STATUS_INIT)
			} else {
				log.Warningf("Unexpected return code %d (%s) for check request for class=%s, hostname=%s", result, result, row.ClassName, d.hostname)
			}
		default:
			log.Warningf("Received unexpected result from phproxyd at %s: type %T, result: %v", d.hostname, rawResp, rawResp)
		}
	}
}

func (d *LauncherData) processRunningTooLong() {
	now := time.Now().Unix()

	for run_id, row := range d.runningMap {
		if !row.max_finished_ts.Valid || row.max_finished_ts.Int64 >= now {
			continue
		}

		if shouldHaveFinished(row) {
			d.logIncorrectFinish(nil, row, RUN_STATUS_RUNNING)
			continue
		}

		_, rawResp, err := d.call(&badoo_phproxyd.RequestCheck{Hash: proto.Uint64(run_id)})
		if err != nil {
			log.Warningf("Could not call check at hostname %s: %s", d.hostname, err.Error())
			continue
		}

		switch resp := rawResp.(type) {
		case *badoo_phproxyd.ResponseCheck:
			d.logIncorrectFinish(resp, row, RUN_STATUS_RUNNING)
		case *badoo_phproxyd.ResponseGeneric:
			code := -badoo_phproxyd.Errno(resp.GetErrorCode())

			if code == badoo_phproxyd.Errno_ERRNO_ALREADY_RUNNING {
				d.terminate(row, KILL_ACTION_NO_ACTION)
			} else if code == badoo_phproxyd.Errno_ERRNO_WAIT_FOR_FREE {
				d.logIncorrectFinish(nil, row, RUN_STATUS_RUNNING)
			} else if code == badoo_phproxyd.Errno_ERRNO_NOT_FOUND {
				d.terminate(row, KILL_ACTION_LOG_SCRIPT_FINISH_RUNNING)
			} else {
				log.Warningf("Unexpected error code %d (%s) from phproxyd at %s (process running too long)", code, resp.GetErrorText(), d.hostname)
			}
		default:
			log.Warningf("Received unexpected result from phproxyd at %s: type %T, result: %v", d.hostname, rawResp, rawResp)
		}
	}
}

func (d *LauncherData) terminate(row *RunQueueEntry, action string) {
	if d.killedRecently[row.Id] {
		return
	}

	if action == KILL_ACTION_NO_ACTION {
		d.call(&badoo_phproxyd.RequestTerminate{Hash: proto.Uint64(row.Id)})
	} else {
		params := []string{
			`\ScriptFramework\Script_Kill`,
			fmt.Sprintf("--force-sf-db=%s", db.GetDbName()),
			fmt.Sprintf("--kill-run-id=%d", row.Id),
			fmt.Sprintf("--kill-action=%s", action),
			fmt.Sprintf("--kill-class-name=%s", row.ClassName),
			fmt.Sprintf("--kill-timetable-id=%d", row.timetable_id.Int64),
		}

		d.call(&badoo_phproxyd.RequestRun{
			Script:       proto.String(getScriptPath(row.settings)),
			Hash:         proto.Uint64(0),
			Tag:          proto.String(PHPROXY_TAG),
			Force:        proto.Int32(1),
			Params:       params,
			Store:        badoo_phproxyd.StoreT_MEMORY.Enum(),
			FreeAfterRun: proto.Bool(true),
		})
	}

	d.killedRecently[row.Id] = true
}

func (d *LauncherData) init() {
	d.waitingMap = make(map[uint64]*RunQueueEntry)
	d.initMap = make(map[uint64]*RunQueueEntry)
	d.runningMap = make(map[uint64]*RunQueueEntry)
	d.finishedMap = make(map[uint64]*RunQueueEntry)
	d.allMap = make(map[uint64]*RunQueueEntry)

	d.killedRecently = make(map[uint64]bool)

	d.deletedIds = make(map[uint64]int)
	d.tickCh = time.After(time.Second)
}
