package jobgen

import (
	"database/sql"
	"fmt"
	"sort"

	"badoo/_packages/log"
	"errors"
	"time"
)

func forceCheckDeletedThread() {
	for {
		time.Sleep(time.Second)

		chans := make([]chan bool, 0)
		func() {
			dispatchThreads.Lock()
			defer dispatchThreads.Unlock()

			for _, rows := range dispatchThreads.v {
				for _, row := range rows {
					chans = append(chans, row.tickCheckDeletedCh)
				}
			}
		}()

		if len(chans) == 0 {
			continue
		}

		slp := time.Duration(10000000/len(chans)) * time.Microsecond
		for _, ch := range chans {
			trigger(ch, "forceCheckDeletedThread")
			time.Sleep(slp)
		}
	}
}

func getOrCreateDispatcherThread(className, location string) *DispatcherData {
	dispatchThreads.Lock()
	defer dispatchThreads.Unlock()

	el, ok := dispatchThreads.v[className]

	if !ok {
		el = make(map[string]*DispatcherData)
		dispatchThreads.v[className] = el
	}

	dt, ok := el[location]

	if !ok {
		dt = &DispatcherData{
			className:          className,
			location:           location,
			newJobsCh:          make(chan *NewJobs, 1),
			redispatchCh:       make(chan bool, 1),
			rusageCh:           make(chan *ScriptRusageEntry, 1),
			finishCh:           make(chan *FinishEvent, 1),
			zeroTTCh:           make(chan bool, 1),
			allJobsCh:          make(chan []*TimetableEntry, 1),
			debugCh:            make(chan *DebugPrintRequest, 1),
			killRequestCh:      make(chan *KillRequest, 1),
			jobsCountCh:        make(chan *JobsCountRequest, 1),
			tickCheckDeletedCh: make(chan bool, 1),
		}

		log.Infof("Creating a goroutine for class=%s and location=%s", className, location)

		el[location] = dt
		go dt.dispatchThread()
	}

	return dt
}

func notifyTTFinished(className, location string, timetable_id, run_id uint64, success, deleteRq bool, prevStatus string) error {
	dt := getOrCreateDispatcherThread(className, location)

	ev := &FinishEvent{
		timetable_id: timetable_id,
		run_id:       run_id,
		prevStatus:   prevStatus,
		success:      success,
		deleteRq:     deleteRq,
		errorCh:      make(chan error, 1),
	}

	dt.finishCh <- ev
	return <-ev.errorCh
}

// location = DEFAULT_LOCATION_IDX for location_type=any and hostname for location_type=each
func notifyAboutNewTTRows(className string, location string, ttRows []*TimetableEntry, allJobs bool) {
	if ttRows == nil || len(ttRows) == 0 {
		return
	}

	dt := getOrCreateDispatcherThread(className, location)

	if allJobs {
		select {
		case dt.allJobsCh <- ttRows:
		default:
		}
	} else {
		respCh := make(chan bool, 1)
		select {
		case dt.newJobsCh <- &NewJobs{rows: ttRows, respCh: respCh}:
			<-respCh
		default:
		}
	}
}

func getOrCreateLauncherThread(hostname string) *LauncherData {
	launcherThreads.Lock()
	defer launcherThreads.Unlock()

	dt, ok := launcherThreads.v[hostname]

	if !ok {
		dt = &LauncherData{
			hostname:              hostname,
			waitingCh:             make(chan *RunQueueEntry, 100),
			allRowsCh:             make(chan []*RunQueueEntry, 1),
			debugCh:               make(chan *LauncherDebugPrintRequest, 1),
			updateStatusRequestCh: make(chan *LauncherUpdateStatusRequest, 5),
			logFinishRequestCh:    make(chan *LauncherLogFinishRequest, 5),
		}

		log.Infof("Creating a launcher goroutine for hostname=%s", hostname)

		launcherThreads.v[hostname] = dt
		go dt.mainThread()
	}

	return dt
}

func notifyAboutNewRQRows(hostname string, rqRows []*RunQueueEntry, allJobs bool) {
	if rqRows == nil || len(rqRows) == 0 {
		return
	}

	dt := getOrCreateLauncherThread(hostname)

	if allJobs {
		select {
		case dt.allRowsCh <- rqRows:
		default:
		}
	} else {
		for _, row := range rqRows {
			select {
			case dt.waitingCh <- row:
			default:
			}
		}
	}
}

func runqueuePeriodicSelectThread() {
	timer := time.Tick(time.Millisecond * time.Duration(ttReloadIntervalMs))

	for {
		<-timer

		if err := selectRQAndNotify(); err != nil {
			log.Warningf("Could not perform periodic timetable select: %s", err.Error())
		}
	}
}

// select everything from timetable, notify dispatcher threads and start them if needed
func notifyForFullTTSelect(classLocTTRows map[string]map[string][]*TimetableEntry, isExisting bool) error {
	settingsIds := make(map[uint64]bool)

	for _, locTTRows := range classLocTTRows {
		for _, ttRows := range locTTRows {
			for _, row := range ttRows {
				settingsIds[row.settings_id] = true
			}
		}
	}

	err := loadNewIds(settingsIds)
	if err != nil {
		return err
	}

	for className, locTTRows := range classLocTTRows {
		// TODO: ensure that "location" is always the same for "any" type of script, otherwise goroutine will receive
		// multiple notifications
		for location, ttRows := range locTTRows {
			anyRows := make([]*TimetableEntry, 0, len(locTTRows))
			eachRows := make([]*TimetableEntry, 0, len(locTTRows))

			allSettingsMutex.Lock()
			for _, row := range ttRows {
				row.settings = allSettings[row.settings_id]
			}
			allSettingsMutex.Unlock()

			for _, row := range ttRows {
				if row.settings == nil {
					log.Warningf("Incorrect row in timetable, settings are invalid: %+v", row)
					continue
				}

				if row.settings.location_type == LOCATION_TYPE_EACH {
					eachRows = append(eachRows, row)
				} else {
					anyRows = append(anyRows, row)
				}
			}

			if len(anyRows) > 0 {
				notifyAboutNewTTRows(className, DEFAULT_LOCATION_IDX, anyRows, isExisting)
			}

			if len(eachRows) > 0 {
				notifyAboutNewTTRows(className, location, eachRows, isExisting)
			}
		}
	}

	return nil
}

// select everything from timetable, notify dispatcher threads and start them if needed
func selectRQAndNotify() error {
	hostRows, err := SelectRunQueue()
	if err != nil {
		return err
	}

	settingsIds := make(map[uint64]bool)

	for _, rows := range hostRows {
		for _, row := range rows {
			settingsIds[row.settings_id] = true
		}
	}

	err = loadNewIds(settingsIds)
	if err != nil {
		return err
	}

	for hostname, rows := range hostRows {
		allSettingsMutex.Lock()
		for _, row := range rows {
			row.settings = allSettings[row.settings_id]
		}
		allSettingsMutex.Unlock()

		notifyAboutNewRQRows(hostname, rows, true)
	}

	return nil
}

func getWaitingRQLen(hostname string) uint64 {
	launcherThreads.Lock()
	defer launcherThreads.Unlock()

	dt, ok := launcherThreads.v[hostname]
	if !ok {
		return 0
	}
	return uint64(len(dt.waitingCh))
}

func pNullFloat64(arg sql.NullFloat64) string {
	if !arg.Valid {
		return "NULL"
	}

	return fmt.Sprintf("%.2f", arg.Float64)
}

func pNullInt64(arg sql.NullInt64) string {
	if !arg.Valid {
		return "NULL"
	}

	return fmt.Sprintf("%d", arg.Int64)
}

func (entry *ServerInfo) String() string {
	return fmt.Sprintf(
		"cip:%-8.0f cppc:%-7d cic:%-5.2f cc:%-2d cp:%-5s mt:%-12s mf:%-12s mc:%-12s mp:%-12s su:%-12s mm:%s mmr:%s",
		entry.cpu_idle_parrots,
		entry.cpu_parrots_per_core,
		entry.cpu_idle_cores,
		entry.cpu_cores,
		pNullFloat64(entry.cpu_parasite),
		pNullInt64(entry.mem_total),
		pNullInt64(entry.mem_free),
		pNullInt64(entry.mem_cached),
		pNullInt64(entry.mem_parasite),
		pNullInt64(entry.swap_used),
		pNullInt64(entry.min_memory),
		pNullFloat64(entry.min_memory_ratio))
}

func printFreeResources(infoMap map[string]*ServerInfo) {
	spisok := make([]string, 0, len(infoMap))
	for hostname := range infoMap {
		spisok = append(spisok, hostname)
	}
	sort.Strings(spisok)
	for _, hostname := range spisok {
		row := infoMap[hostname]
		log.Printf("freeresources: host: %-5s serverInfo:%s", hostname, row)
	}
}

type DispatcherThreadDescr struct {
	ClassName string
	Location  string
}

func GetDispatcherThreadsList() []DispatcherThreadDescr {
	dispatchThreads.Lock()
	defer dispatchThreads.Unlock()

	result := make([]DispatcherThreadDescr, 0)

	for className, locs := range dispatchThreads.v {
		for location, _ := range locs {
			result = append(result, DispatcherThreadDescr{ClassName: className, Location: location})
		}
	}

	return result
}

func GetLauncherThreadsList() []string {
	launcherThreads.Lock()
	defer launcherThreads.Unlock()

	result := make([]string, 0)

	for hostname, _ := range launcherThreads.v {
		result = append(result, hostname)
	}

	return result
}

func GetKillerThreadsList() []string {
	killerThreads.Lock()
	defer killerThreads.Unlock()

	result := make([]string, 0)

	for className, _ := range killerThreads.v {
		result = append(result, className)
	}

	return result
}

func GetLauncherJobs(hostname string) (*LauncherState, error) {
	ch, err := getLauncherDebugCh(hostname)
	if err != nil {
		return nil, err
	}

	respCh := make(chan *LauncherState, 1)
	select {
	case ch <- &LauncherDebugPrintRequest{RespCh: respCh}:
	default:
		return nil, errors.New("Debug channel for the goroutine is busy")
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(time.Second * 60):
		return nil, errors.New("Debug request timeout")
	}
}

func GetDispatcherJobs(className, location string) (*JobGenState, error) {
	ch, err := getDispatcherDebugCh(className, location)
	if err != nil {
		return nil, err
	}

	respCh := make(chan *JobGenState, 1)
	select {
	case ch <- &DebugPrintRequest{Waiting: true, Added: true, RespCh: respCh}:
	default:
		return nil, errors.New("Debug channel for the goroutine is busy")
	}

	select {
	case resp := <-respCh:
		return resp, nil
	case <-time.After(time.Second * 60):
		return nil, errors.New("Debug request timeout")
	}
}

func getDispatcherDebugCh(className, location string) (chan *DebugPrintRequest, error) {
	dispatchThreads.Lock()
	defer dispatchThreads.Unlock()

	classEl, ok := dispatchThreads.v[className]
	if !ok {
		return nil, fmt.Errorf("No such class: %s", className)
	}

	el, ok := classEl[location]
	if !ok {
		return nil, fmt.Errorf("No such location '%s' for class %s", location, className)
	}

	return el.debugCh, nil
}

func getDispatcherJobsCountCh(className, location string) chan *JobsCountRequest {
	dispatchThreads.Lock()
	defer dispatchThreads.Unlock()

	classEl, ok := dispatchThreads.v[className]
	if !ok {
		return nil
	}

	el, ok := classEl[location]
	if !ok {
		return nil
	}

	return el.jobsCountCh
}

func getLauncherDebugCh(hostname string) (chan *LauncherDebugPrintRequest, error) {
	launcherThreads.Lock()
	defer launcherThreads.Unlock()

	el, ok := launcherThreads.v[hostname]
	if !ok {
		return nil, fmt.Errorf("No such hostname: %s", hostname)
	}

	return el.debugCh, nil
}

func getLauncherUpdateStatusCh(hostname string) (chan *LauncherUpdateStatusRequest, error) {
	launcherThreads.Lock()
	defer launcherThreads.Unlock()

	el, ok := launcherThreads.v[hostname]
	if !ok {
		return nil, fmt.Errorf("No such hostname: %s", hostname)
	}

	return el.updateStatusRequestCh, nil
}

func getLauncherLogFinishCh(hostname string) (chan *LauncherLogFinishRequest, error) {
	launcherThreads.Lock()
	defer launcherThreads.Unlock()

	el, ok := launcherThreads.v[hostname]
	if !ok {
		return nil, fmt.Errorf("No such hostname: %s", hostname)
	}

	return el.logFinishRequestCh, nil
}
