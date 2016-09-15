package jobgen

import (
	"container/heap"
	"time"

	"badoo/_packages/log"
	"github.com/badoo/thunder/db"
	"bytes"
	"crypto/md5"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"
)

// TTPriorityQueue implements heap.Interface and holds Items.
type TTPriorityQueue []*TimetableEntry

type KillRequest struct {
	ResCh chan error // response for request (success or failure)
}

type FinishEvent struct {
	timetable_id        uint64
	run_id              uint64
	prevStatus          string // iff run_id != 0
	success             bool
	isInitial           bool
	deleteRq            bool
	havePrevFinishCount bool
	prevFinishCount     uint // previous value for finish_count (for external finishes it should be current value)
	errorCh             chan error
}

type JobGenState struct {
	Added   []*TimetableEntry
	Waiting []*TimetableEntry

	RawResp string
}

type DebugPrintRequest struct {
	Waiting bool
	Added   bool

	RespCh chan *JobGenState
}

type JobsCountRequest struct {
	RespCh chan int
}

type NewJobs struct {
	rows   []*TimetableEntry
	respCh chan bool // channel for initial loading from DB
}

type DispatcherData struct {
	// setup params
	className          string
	location           string
	newJobsCh          chan *NewJobs
	redispatchCh       chan bool
	rusageCh           chan *ScriptRusageEntry
	finishCh           chan *FinishEvent
	zeroTTCh           chan bool              // channel to notify generator that there are no more jobs to dispatch
	allJobsCh          chan []*TimetableEntry // all jobs are periodically selected from timetable so that silently finished jobs can be detected
	debugCh            chan *DebugPrintRequest
	killRequestCh      chan *KillRequest
	jobsCountCh        chan *JobsCountRequest
	tickCheckDeletedCh chan bool // check deleted channel

	rusage *ScriptRusageEntry // can be nil if no rusage is known

	// state
	waitingList      TTPriorityQueue
	waitingMap       map[uint64]*TimetableEntry // index for waitingList
	addedMap         map[uint64]*TimetableEntry // jobs that were already added to queue
	addedJobData     map[string]uint64          // job_data => timetable_id
	killRequest      *KillRequest
	deletedIds       map[uint64]int   // ttId => refCount
	tickRedispatchCh <-chan time.Time // redispatch on tick (e.g. upon TTL expire or selectHostname() failure)
}

func copyTTEntries(entries []*TimetableEntry) []*TimetableEntry {
	res := make([]*TimetableEntry, 0, len(entries))

	for _, origV := range entries {
		v := new(TimetableEntry)
		*v = *origV
		res = append(res, v)
	}

	return res
}

func copyTTEntriesMap(entries map[uint64]*TimetableEntry) []*TimetableEntry {
	res := make([]*TimetableEntry, 0, len(entries))

	for _, origV := range entries {
		v := new(TimetableEntry)
		*v = *origV
		res = append(res, v)
	}

	return res
}

func (d *DispatcherData) dispatchThread() {
	d.init()

	for {
		killReqCh := d.killRequestCh
		if d.killRequest != nil {
			killReqCh = nil
		}

		select {
		case ev := <-d.newJobsCh:
			d.processNewJobsEv(ev)
			trigger(ev.respCh, "ev.respCh")
		case jobs := <-d.allJobsCh:
			d.processAllJobs(jobs)
		case <-d.redispatchCh:
			d.redispatch()
		case <-d.tickRedispatchCh:
			d.tickRedispatchCh = nil
			d.redispatch()
		case <-d.tickCheckDeletedCh:
			d.checkDeleted()
		case rusage := <-d.rusageCh:
			d.rusage = rusage
		case ev := <-d.finishCh:
			d.processFinished(ev)
		case req := <-killReqCh:
			d.killRequest = req
			trigger(d.redispatchCh, "redispatch")
		case req := <-d.jobsCountCh:
			req.RespCh <- len(d.addedMap) + len(d.waitingMap)
		case ev := <-d.debugCh:
			d.processDebugEvent(ev)
		}
	}
}

func (d *DispatcherData) checkDeleted() {
	checkIds := make([]uint64, 0, len(d.waitingMap)+len(d.addedMap))

	for _, row := range d.waitingMap {
		checkIds = append(checkIds, row.id)
	}

	for _, row := range d.addedMap {
		checkIds = append(checkIds, row.id)
	}

	existing, err := getExistingTTIds(checkIds)
	if err != nil {
		log.Warnf("Could not check existing tt ids for class=%s and location=%s: %s", d.className, d.location, err.Error())
		return
	}

	for _, row := range d.waitingMap {
		if !existing[row.id] {
			log.Warnf("Timetable row disappeared from waiting state in DB: %+v", row)
			d.removeFromWaiting(row)
		}
	}

	for _, row := range d.addedMap {
		if !existing[row.id] {
			log.Warnf("Timetable row disappeared from added state in DB: %+v", row)
			d.removeFromAdded(row)
		}
	}
}

func (d *DispatcherData) processNewJobsEv(ev *NewJobs) {
	ids := make([]uint64, 0, len(ev.rows))
	settingsMap := make(map[uint64]*ScriptSettings)

	for _, row := range ev.rows {
		if row.settings == nil {
			log.Warningf("Incorrect row in timetable (processNewJobsEv), settings are invalid: %+v", row)
			continue
		}

		if d.waitingMap[row.id] != nil || d.addedMap[row.id] != nil {
			continue
		}

		settingsMap[row.id] = row.settings
		ids = append(ids, row.id)
	}

	rows, err := selectTimetableByIds(ids)
	if err != nil {
		log.Warnf("could not select tt_enties for ids:%+v err:%s", ids, err.Error())
		return
	}

	for _, row := range rows {
		row.settings = settingsMap[row.id]
	}

	d.acceptNewJobs(rows)
}

func (d *DispatcherData) processDebugEvent(ev *DebugPrintRequest) {
	resp := new(JobGenState)

	var b bytes.Buffer

	fmt.Fprintf(&b, "Asked to print state of goroutine class=%s, location=%s\n", d.className, d.location)

	if ev.Added {
		fmt.Fprintf(&b, "addedJobData: %+v\n", d.addedJobData)
		for tt_id, row := range d.addedMap {
			fmt.Fprintf(&b, "  #%d: %+v\n", tt_id, row)
		}

		resp.Added = copyTTEntriesMap(d.addedMap)
	}

	fmt.Fprintf(&b, "\n")

	if ev.Waiting {
		fmt.Fprintf(&b, "waitingList: %+v\n", d.waitingList)
		for tt_id, row := range d.waitingMap {
			fmt.Fprintf(&b, "  #%d: %+v\n", tt_id, row)
		}

		resp.Waiting = copyTTEntries(d.waitingList)
	}

	fmt.Fprintf(&b, "\n")

	fmt.Fprintf(&b, "Deleted ids count: %d\n", len(d.deletedIds))
	fmt.Fprintf(&b, "Tick redispatch channel: %v\n", d.tickRedispatchCh)
	fmt.Fprintf(&b, "Kill request: %v\n", d.killRequest)

	resp.RawResp = b.String()

	select {
	case ev.RespCh <- resp:
	default:
	}
}

func (d *DispatcherData) processAllJobs(jobs []*TimetableEntry) {
	newJobs := make([]*TimetableEntry, 0)
	toProcessFinished := make([]*FinishEvent, 0)

	for _, row := range jobs {
		if el, ok := d.waitingMap[row.id]; ok {

			if row.finish_count >= el.finish_count && row.added_to_queue_ts.Valid && !row.finished_ts.Valid {
				d.removeFromWaiting(el)
				row.reportedDup = el.reportedDup
				d.addToAdded(row)

				log.Warnf("external waiting to added promotion OLD:%+v---NEW:%+v", el, row)
			} else if row.finish_count > el.finish_count && !row.added_to_queue_ts.Valid {
				d.removeFromWaiting(el)
				d.addToWaiting(row)
				log.Warnf("external finish_count promotion OLD:%+v---NEW:%+v", el, row)
			}
			continue
		}

		if el, ok := d.addedMap[row.id]; ok {
			// External job notification can update timetable by incrementing it's finish count.
			// We do not really care about any modifications made by the script except for finished_successfully field
			if row.finish_count > el.finish_count {
				log.Debugf("External finish for tt row %+v", row)
				toProcessFinished = append(toProcessFinished, &FinishEvent{
					timetable_id: row.id, success: row.finished_successfully != 0,
					havePrevFinishCount: true, prevFinishCount: row.finish_count, errorCh: make(chan error, 1)})
			}
		} else if d.deletedIds[row.id] == 0 {
			log.Debugf("External timetable row %+v", row)
			newJobs = append(newJobs, row)
		}
	}

	if len(newJobs) > 0 {
		d.acceptNewJobs(newJobs)
	}

	for _, el := range toProcessFinished {
		log.Debugf("processFinished: %+v", el)
		d.processFinished(el)
	}

	for ttId, refCount := range d.deletedIds {
		if refCount--; refCount <= 0 {
			delete(d.deletedIds, ttId)
		} else {
			d.deletedIds[ttId] = refCount
		}
	}
}

// process finished:
// 1. send an error to ev.errorCh, nil if all is ok
// 2. restore state upon failure
func (d *DispatcherData) processFinished(ev *FinishEvent) {
	var err error
	defer func() { ev.errorCh <- err }()

	row, ok := d.addedMap[ev.timetable_id]
	if !ok {

		if rowWaiting, ok := d.waitingMap[ev.timetable_id]; ok {
			log.Warningf("Got 'finished' event about waiting timetable_id: %d, class=%s, location=%s, row=%+v", ev.timetable_id, d.className, d.location, rowWaiting)
			err = fmt.Errorf("timetable id is waiting: %d, class=%s, location=%s", ev.timetable_id, d.className, d.location)
		} else {
			log.Warningf("Got 'finished' event about unknown timetable_id: %d, class=%s, location=%s", ev.timetable_id, d.className, d.location)
			err = fmt.Errorf("Unknown timetable id: %d, class=%s, location=%s", ev.timetable_id, d.className, d.location)
		}
		return
	}

	now := uint64(time.Now().Unix())

	// restore everything in case of error
	rowCopy := *row
	defer func() {
		if err != nil {
			log.Warnf("Restoring old tt row (error: %s) from %+v => %+v", err.Error(), row, rowCopy)
			*row = rowCopy
		} else {
			// TODO: update rusage estimation
		}
	}()

	if !ev.isInitial {
		if ev.success {
			row.finished_successfully = 1
		} else {
			row.finished_successfully = 0
		}

		row.finish_count++
		if !ev.success {
			row.retry_count++
		} else {
			row.retry_count = 0
		}
	}

	row.NextLaunchTs.Valid = false
	row.NextLaunchTs.Int64 = 0

	var ttl uint32
	if row.method == METHOD_RUN {
		ttl = row.settings.ttl
	}

	// we should not delete entries that have ttl > 0 and have hit max retries because there is "repeat" field still
	shouldDelete := d.killRequest != nil ||
		(ttl == 0 && (ev.success || row.retry_count >= row.settings.max_retries)) ||
		(ttl > 0 && now > row.created+uint64(ttl))

	cb := func(tx *db.LazyTrx) error {
		var err error

		if ev.run_id != 0 {
			if ev.deleteRq {
				err = deleteFromRunQueue(tx, []uint64{ev.run_id}, ev.prevStatus)
			} else {
				err = errors.New("unexpected deleteRq value")
			}

			if err != nil {
				return err
			}
		}

		if shouldDelete {
			return deleteAddedFromTimetable(tx, []uint64{ev.timetable_id})
		}

		return logTTFinish(tx, row, ev.havePrevFinishCount, ev.prevFinishCount)
	}

	if shouldDelete {
		if err = db.DoInLazyTransaction(cb); err == nil {
			if row.id != ev.timetable_id {
				log.Warnf("Inconsistency of addedMap[%d] = row = %+v", ev.timetable_id, row)
				row.id = ev.timetable_id
			}
			d.removeFromAdded(row)
			d.deletedIds[ev.timetable_id] = DELETE_IDS_KEEP_GENERATIONS

			d.checkZero("processFinished")

			trigger(d.redispatchCh, "redispatch")
		} else {
			log.Warnf("could not process finished: %s", err.Error())
		}

		return
	}

	next_launch_ts := int64(now)

	if ev.success && row.added_to_queue_ts.Valid {
		next_launch_ts = row.added_to_queue_ts.Int64 + row.repeat.Int64
	} else if !ev.success {
		if row.retry_count < 3 {
			next_launch_ts += int64(row.default_retry)
		} else {
			e := row.retry_count - 2
			if e >= 3 {
				e = 3
			}

			next_launch_ts += (1 << e) * int64(row.default_retry)
		}
	}

	row.NextLaunchTs.Valid = true
	row.NextLaunchTs.Int64 = next_launch_ts

	row.finished_ts.Valid = false
	row.finished_ts.Int64 = 0

	row.added_to_queue_ts.Valid = false
	row.added_to_queue_ts.Int64 = 0

	if err = db.DoInLazyTransaction(cb); err == nil {
		d.removeFromAdded(row)
		d.addToWaiting(row)

		trigger(d.redispatchCh, "redispatch")
	}
}

func (d *DispatcherData) checkZero(src string) {
	if len(d.addedMap) == 0 && d.waitingList.Len() == 0 {
		log.Debugf("No rows left in class=%s, location=%s (%s)", d.className, d.location, src)
		trigger(d.zeroTTCh, "zerott")

		if d.killRequest != nil {
			log.Printf("Killed all jobs in class=%s, location=%s, waiting on continue channel", d.className, d.location)
			d.killRequest.ResCh <- nil
			d.killRequest = nil
			log.Printf("Can continue dispatching in class=%s, location=%s", d.className, d.location)
		}
	}
}

func (d *DispatcherData) printTrace() {
	trace := make([]byte, 8192)
	n := runtime.Stack(trace, false)
	log.Debugf("class:%s location:%s\nStack trace: %s\n", d.className, d.location, trace[0:n])
}

func (d *DispatcherData) addToAdded(row *TimetableEntry) {
	if el, ok := d.addedMap[row.id]; ok {
		log.Warnf("attempted to add already added OLD:%+v NEW:%+v", el, row)
		d.printTrace()
		return
	}
	if el, ok := d.waitingMap[row.id]; ok {
		log.Warnf("attempted to add already waiting OLD:%+v NEW:%+v", el, row)
		d.printTrace()
		return
	}
	d.addedMap[row.id] = row
	d.addedJobData[row.JobData] = row.id
}

func (d *DispatcherData) removeFromAdded(row *TimetableEntry) {
	if _, ok := d.addedMap[row.id]; !ok {
		log.Warnf("attempted to unadd nonexistent ROW:%+v", row)
		d.printTrace()
		return
	}
	if el, ok := d.waitingMap[row.id]; ok {
		log.Warnf("attempted to unadd waiting OLD:%+v NEW:%+v", el, row)
		d.printTrace()
		return
	}

	delete(d.addedMap, row.id)
	delete(d.addedJobData, row.JobData)
}

func (d *DispatcherData) addToWaiting(row *TimetableEntry) {
	if el, ok := d.addedMap[row.id]; ok {
		log.Warnf("attempted to wait already added OLD:%+v NEW:%+v", el, row)
		d.printTrace()
		return
	}
	if el, ok := d.waitingMap[row.id]; ok {
		log.Warnf("attempted to wait already waiting OLD:%+v NEW:%+v", el, row)
		d.printTrace()
		return
	}
	heap.Push(&d.waitingList, row)
	d.waitingMap[row.id] = row
}

func (d *DispatcherData) removeFromWaiting(row *TimetableEntry) {
	if _, ok := d.waitingMap[row.id]; !ok {
		log.Warnf("attempted to unwait nonexistent ROW:%+v", row)
		d.printTrace()
		return
	}
	if el, ok := d.addedMap[row.id]; ok {
		log.Warnf("attempted to unwait added OLD:%+v NEW:%+v", el, row)
		d.printTrace()
		return
	}
	heap.Remove(&d.waitingList, row.index)
	delete(d.waitingMap, row.id)
}

func (d *DispatcherData) acceptNewJobs(jobs []*TimetableEntry) {
	for _, row := range jobs {
		if row.settings == nil {
			buf := make([]byte, 5000)
			n := runtime.Stack(buf, false)
			log.Warningf("Incorrect row in timetable (acceptNewJobs), settings are invalid: %+v\ntrace:%s", row, buf[0:n])
			continue
		}

		// actualization from DB
		if row.added_to_queue_ts.Valid {
			d.addToAdded(row)
			if row.finished_ts.Valid && row.finish_count > 0 {
				row.finish_count--
				d.processFinished(&FinishEvent{timetable_id: row.id,
					success: row.finished_successfully != 0, errorCh: make(chan error, 1)})
			}
		} else {
			d.addToWaiting(row)
		}
	}

	trigger(d.redispatchCh, "redispatch")
}

func getHostnameIdx(hostname string) uint32 {
	h := md5.New()
	h.Write([]byte(hostname))
	res := h.Sum(nil)

	launcherInstanceCount := 32
	if isDevelServer {
		launcherInstanceCount = 8
	}

	instances := uint32(launcherInstanceCount)
	if !isDevelServer {
		instances = uint32(launcherInstanceCount - 1)
	}

	// all migration hosts will be served by a separate launcher so that they do not affect anything else

	hostname_idx := uint32((uint32(res[0])<<24 + uint32(res[1])<<16 + uint32(res[2])<<8 + uint32(res[3])) % instances)
	if strings.HasPrefix(hostname, "cloudmigration") && !isDevelServer {
		hostname_idx = uint32(launcherInstanceCount - 1)
	} else if strings.HasPrefix(hostname, "cloud") { // shard by clouds using round-robin
		number, err := strconv.Atoi(strings.TrimPrefix(hostname, "cloud"))
		if err == nil {
			hostname_idx = uint32(number) % instances
		}
	}

	return hostname_idx
}

func (d *DispatcherData) redispatch() {
	returnToWaitingList := make([]*TimetableEntry, 0)
	defer func() {
		for _, row := range returnToWaitingList {
			d.addToWaiting(row)
		}
	}()

	now := uint64(time.Now().Unix())

	newRqList := make([]*RunQueueEntry, 0)
	toDeleteFromWaitingList := make([]*TimetableEntry, 0)

	for l := d.waitingList.Len(); l > 0; l-- {
		row := heap.Pop(&d.waitingList).(*TimetableEntry)
		delete(d.waitingMap, row.id)

		if d.killRequest != nil {
			toDeleteFromWaitingList = append(toDeleteFromWaitingList, row)
			continue
		}

		if uint64(row.NextLaunchTs.Int64) > now {
			d.tickRedispatchCh = time.After(time.Second * time.Duration(uint64(row.NextLaunchTs.Int64)-now))
			returnToWaitingList = append(returnToWaitingList, row)
			break
		}

		if len(d.addedMap) >= row.settings.instance_count {
			returnToWaitingList = append(returnToWaitingList, row)
			break
		}

		if _, ok := d.addedJobData[row.JobData]; ok {
			if !row.reportedDup {
				log.Warningf("Duplicate job %s for class %s and location %s", row.JobData, d.className, row.location)
				row.reportedDup = true
			}

			returnToWaitingList = append(returnToWaitingList, row)
			continue
		}

		if row.method == METHOD_RUN && row.settings.ttl > 0 && now > row.created+uint64(row.settings.ttl) {
			if row.finish_count == 0 {
				log.Warningf("Job expired before being run even once: job %s for class %s and location %s", row.JobData, d.className, row.location)
			}
			toDeleteFromWaitingList = append(toDeleteFromWaitingList, row)
			continue
		}

		// do not try to dispatch next ones if selectHostname failed, and do not forget to return the row as well
		hostname, err := selectHostname(row.location, row.settings.location_type, d.rusage.cpu_usage, d.rusage.max_memory)
		if err != nil {
			logFailedLocation(row.settings, row.location, err.Error())
			d.tickRedispatchCh = time.After(time.Second)
			returnToWaitingList = append(returnToWaitingList, row)
			break
		} else {
			settings := row.settings
			if settings.location_type == LOCATION_TYPE_ANY && (settings.developer.String != "") && (settings.developer.String != "wwwrun") && ((now - uint64(settings.created)) <= DEVELOPER_CUSTOM_PATH_TIMEOUT) {
				hostname = DEVELOPER_DEBUG_HOSTNAME
			}
			log.Debugln("Selected ", hostname, " for ", row.location, " (loc_type=", settings.location_type, ")")
		}

		nullNow := sql.NullInt64{Valid: true, Int64: int64(now)}

		queueRow := &RunQueueEntry{
			ClassName:      d.className,
			timetable_id:   sql.NullInt64{Valid: true, Int64: int64(row.id)},
			generation_id:  row.generation_id,
			hostname:       hostname,
			hostname_idx:   getHostnameIdx(hostname),
			JobData:        row.JobData,
			method:         row.method,
			created:        nullNow,
			RunStatus:      RUN_STATUS_WAITING,
			waiting_ts:     nullNow,
			should_init_ts: nullNow,
			token:          row.token,
			retry_attempt:  row.retry_count,
			settings_id:    row.settings_id,
			settings:       row.settings,
		}

		newRqList = append(newRqList, queueRow)

		row.added_to_queue_ts.Valid = true
		row.added_to_queue_ts.Int64 = int64(now)

		d.addToAdded(row)
	}

	err := db.DoInLazyTransaction(func(tx *db.LazyTrx) error {
		return addToQueueAndDeleteExpired(tx, newRqList, toDeleteFromWaitingList)
	})

	if err == nil {
		for _, row := range toDeleteFromWaitingList {
			d.deletedIds[row.id] = DELETE_IDS_KEEP_GENERATIONS
		}

		// all rows can expire by TTL in this loop, so check if it the case and notify job generator about it
		if len(toDeleteFromWaitingList) > 0 {
			d.checkZero("redispatch")
		}

		if len(newRqList) > 0 {
			perHost := make(map[string][]*RunQueueEntry)
			for _, row := range newRqList {
				perHost[row.hostname] = append(perHost[row.hostname], row)
			}

			for hostname, rows := range perHost {
				notifyAboutNewRQRows(hostname, rows, false)
			}
		}

		return
	}

	d.tickRedispatchCh = time.After(time.Second)

	// restore internal structures back in case of error
	log.Warnf("Could not add to run queue for class %s and location %s to database: %s", d.className, d.location, err.Error())

	for _, rqRow := range newRqList {
		row, ok := d.addedMap[uint64(rqRow.timetable_id.Int64)]

		if ok {
			row.added_to_queue_ts.Valid = false
			row.added_to_queue_ts.Int64 = 0
			row.id = uint64(rqRow.timetable_id.Int64)
			d.removeFromAdded(row)
			d.addToWaiting(row)
		} else {
			log.Warnf("Internal consistency error: could not find row with timetable id %d", rqRow.timetable_id)
		}
	}
}

func (d *DispatcherData) init() {
	if d.rusage == nil {
		d.rusage = &ScriptRusageEntry{cpu_usage: defaultRusage, max_memory: defaultMaxMemory}
	}

	d.waitingList = make(TTPriorityQueue, 0)
	d.waitingMap = make(map[uint64]*TimetableEntry)
	d.addedMap = make(map[uint64]*TimetableEntry)
	d.addedJobData = make(map[string]uint64)
	d.deletedIds = make(map[uint64]int)

	heap.Init(&d.waitingList)
}

// Timetable Priority Queue:

func (pq TTPriorityQueue) Len() int { return len(pq) }

func (pq TTPriorityQueue) Less(i, j int) bool {
	return pq[i].NextLaunchTs.Int64 < pq[j].NextLaunchTs.Int64
}

func (pq TTPriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *TTPriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*TimetableEntry)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *TTPriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// trigger a channel. do not block if channel is already full
func trigger(ch chan<- bool, what string) {
	/*
		buf := make([]byte, 1000)
		n := runtime.Stack(buf, false)

		log.Infof("Triggering %s from %s", what, buf[0:n])
	*/

	select {
	case ch <- true:
	default:
	}
}
