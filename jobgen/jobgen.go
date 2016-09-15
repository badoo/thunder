package jobgen

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"time"

	"badoo/_packages/log"
	"github.com/badoo/thunder/common"
	"github.com/badoo/thunder/db"
)

const (
	CYCLE_CLASS_NAME = "ScriptFramework\\Script_JobGenerator"
	PINBA_CLASS_NAME = "\\" + CYCLE_CLASS_NAME

	JOBS_TYPE_NONE      = "none"
	JOBS_TYPE_INSTANCES = "instances"
	JOBS_TYPE_RANGE     = "range"
	JOBS_TYPE_CUSTOM    = "custom"

	LOCATION_TYPE_ANY  = "any"
	LOCATION_TYPE_EACH = "each"

	LOCATION_ALL = "*"

	METHOD_RUN         = "run"
	METHOD_INIT_JOBS   = "initJobs"
	METHOD_FINISH_JOBS = "finishJobs"

	RUN_STATUS_WAITING  = "Waiting"
	RUN_STATUS_INIT     = "Init"
	RUN_STATUS_RUNNING  = "Running"
	RUN_STATUS_FINISHED = "Finished"

	DEFAULT_LOCATION_IDX = "0"

	HOSTS_UPDATE_INTERVAL = time.Second * 5

	DELETE_IDS_KEEP_GENERATIONS = 3 // buffered channel (1) + processing (1) + selecting (1)

	DEVELOPER_DEBUG_HOSTNAME = "www1"

	SELECT_HOSTNAME_MAX_WAITING_LEN = 5
	THROTTLE_CHAN_CAPACITY          = 5
)

type (
	TimetableEntry struct {
		id                    uint64
		generation_id         sql.NullInt64
		class_name            string
		repeat                sql.NullInt64
		retry_count           uint32
		default_retry         uint32
		JobData               string
		method                string
		location              string
		finished_ts           sql.NullInt64
		finished_successfully int
		finish_count          uint
		NextLaunchTs          sql.NullInt64
		added_to_queue_ts     sql.NullInt64
		token                 string
		settings_id           uint64
		settings              *ScriptSettings
		created               uint64

		// internal state fields
		index       int  // index for priority queue
		reportedDup bool // whether or not we already reported this job as being duplicate
	}

	ScriptEntry struct {
		class_name  string
		settings_id uint64
		settings    *ScriptSettings
	}

	RunQueueEntry struct {
		Id                  uint64
		ClassName           string
		timetable_id        sql.NullInt64
		generation_id       sql.NullInt64
		hostname            string
		hostname_idx        uint32
		JobData             string
		method              string
		RunStatus           string
		created             sql.NullInt64
		waiting_ts          sql.NullInt64
		should_init_ts      sql.NullInt64
		init_attempts       uint32
		init_ts             sql.NullInt64
		running_ts          sql.NullInt64
		max_finished_ts     sql.NullInt64
		finished_ts         sql.NullInt64
		stopped_employee_id sql.NullInt64
		token               string
		retry_attempt       uint32
		settings_id         uint64
		settings            *ScriptSettings
	}

	JobInfoEntry struct {
		generation_id        uint64
		class_name           string
		location             string
		init_jobs_ts         sql.NullInt64
		jobs_generated_ts    sql.NullInt64
		finish_jobs_ts       sql.NullInt64
		next_generate_job_ts sql.NullInt64
		stop_requested_ts    sql.NullInt64
		stopped_ts           sql.NullInt64
		settings_id          uint64
		settings             *ScriptSettings
	}

	FlagEntry struct {
		class_name               string
		run_requested_ts         sql.NullInt64
		run_accepted_ts          sql.NullInt64
		pause_requested_ts       sql.NullInt64
		kill_requested_ts        sql.NullInt64
		kill_request_employee_id sql.NullInt64
		run_queue_killed_ts      sql.NullInt64
		killed_ts                sql.NullInt64
		paused_ts                sql.NullInt64
	}

	LoadStateFunc struct {
		name string
		fun  func() error
	}
)

var (
	cycleMs                int64
	ttReloadIntervalMs     int64
	autoIncrementIncrement int64
	isDevelServer          bool

	throttle struct {
		c             chan bool          // read events from c to throttle yourself
		setIntervalCh chan time.Duration // channel to set interval for throttle events
	}

	scriptsMap struct {
		v map[string]*ScriptEntry
		sync.Mutex
	}

	// information about currently dispatching threads
	dispatchThreads struct {
		v map[string]map[string]*DispatcherData // class_name => map(location => dispatcher data))
		sync.Mutex
	}

	launcherThreads struct {
		v map[string]*LauncherData // hostname => launcher data
		sync.Mutex
	}
)

func (p *TimetableEntry) String1() string {
	return fmt.Sprintf("id:%d, class=%s, jd=%s fs:%v fc:%d loc:%s added:%+v", p.id, p.class_name, p.JobData, p.finished_successfully, p.finish_count, p.location, p.added_to_queue_ts)
}

func loadFullState(funcs ...*LoadStateFunc) (err error) {
	for _, funEntry := range funcs {
		startTs := time.Now().UnixNano()
		err = funEntry.fun()

		if err != nil {
			log.Errorf("Could not load %s: %s", funEntry.name, err.Error())
			return err
		}

		log.Debugf("Selected from %s for %.5f sec", funEntry.name, float64(time.Now().UnixNano()-startTs)/1e9)
	}

	return nil
}

// haveTTRows must be nil if there are no timetable entries for any location
// otherwise it must have only true entries like map["location"] => true
// probably jobs generation can be simplified, it is just the way it is
func generateJobs(tx *db.LazyTrx, className string, settings *ScriptSettings, jiRows map[string]*JobInfoEntry, haveTTRows map[string]bool, flags *FlagEntry) (add_to_timetable []*TimetableEntry, err error) {
	if haveTTRows != nil && len(haveTTRows) == 0 {
		haveTTRows = nil
	}

	now := time.Now().Unix()

	add_to_timetable = make([]*TimetableEntry, 0)
	add_job_info := make([]*JobInfoEntry, 0)
	set_finish_jobs := make([]string, 0)
	set_init_jobs := make([]string, 0)
	set_jobs_generated_js := make([]string, 0)
	prepare_next_generation := make([]NextGenParams, 0)

	have_finish_jobs := settings.jobs.Have_finish_jobs
	is_any := (settings.location_type == LOCATION_TYPE_ANY)
	is_temporary := settings.jobs.Temporary
	temporary_can_run := false

	if flags != nil {
		if flags.kill_requested_ts.Valid {
			is_done := (haveTTRows == nil)
			if is_done {
				log.Printf("Class %s is done, all is ok", className)

				if !flags.killed_ts.Valid {
					tx.AddCommitCallback(func() { continueDispatchAfterKill(className) })
					if err = setKilledFlag(tx, className); err != nil {
						return
					}

					if err = prepareNextGeneration(tx, have_finish_jobs, className, settings); err != nil {
						return
					}
				}
			} else {
				log.Printf("Class %s is not done", className)

				startKilling(className)

				// not the best place to put it, but it works
				if err = setMaxFinishedTs(tx, className, flags.kill_request_employee_id.Int64, flags.kill_requested_ts.Int64); err != nil {
					return
				}
			}

			return
		}

		// Stop generating new job generations when we are on pause
		if flags.pause_requested_ts.Valid {
			is_done := generationFinished(className, haveTTRows, jiRows, settings)

			if is_done && !flags.paused_ts.Valid {
				if err = setPausedFlag(tx, className); err != nil {
					return
				}

				flags.paused_ts = sql.NullInt64{Int64: now, Valid: true}
			}

			if !is_any || flags.paused_ts.Valid {
				return
			}
		}

		if is_temporary && flags.run_requested_ts.Valid && is_any {
			// We accepted run request, which means that we already generated jobs
			if flags.run_accepted_ts.Valid {
				if generationFinished(className, haveTTRows, jiRows, settings) {
					if err = resetRunRequest(tx, className); err != nil {
						return
					}

					if err = prepareNextGeneration(tx, have_finish_jobs, className, settings); err != nil {
						return
					}

					return
				}
			} else {
				if err = setRunAccepted(tx, className); err != nil {
					return
				}
			}

			temporary_can_run = true
		}
	}

	if is_temporary && !temporary_can_run || settings.jobs.Type == JOBS_TYPE_NONE {
		return
	}

	locations := make([]string, 0)

	if !is_any {
		all_locations := getLocations(settings)
		timetable_locations := make(map[string]bool)

		if haveTTRows != nil {
			for location, _ := range haveTTRows {
				timetable_locations[location] = true
			}
		}

		// there can be failed hosts that are still running: we must really compare host names, not just counts
		for _, loc := range all_locations {
			if _, ok := timetable_locations[loc]; !ok {
				locations = append(locations, loc)
			}
		}

		if len(locations) == 0 {
			return
		}
	} else {
		if haveTTRows != nil && len(haveTTRows) > 0 {
			return
		}

		locations = getLocations(settings)
	}

	tt_location_type := LOCATION_TYPE_EACH
	if is_any {
		tt_location_type = LOCATION_TYPE_ANY
	}

	for _, location := range locations {
		job_info_key, gliErr := getLocationIdx(tt_location_type, location)
		if gliErr != nil {
			log.Warningf("Error getting location index for %s for location_type %s and location %s: %s", className, tt_location_type, location, gliErr.Error())
			continue
		}

		var row *JobInfoEntry

		if jiRows == nil || jiRows[job_info_key] == nil {
			row = &JobInfoEntry{generation_id: 0,
				class_name:           className,
				location:             job_info_key,
				next_generate_job_ts: sql.NullInt64{Int64: int64(getNextJobGenerateTs(className, true, 0, settings)), Valid: true},
				settings_id:          settings.id}

			add_job_info = append(add_job_info, row)
		} else {
			row = jiRows[job_info_key]
		}

		tt_row := &TimetableEntry{
			class_name:            className,
			default_retry:         settings.retry_job,
			repeat:                settings.repeat_job,
			method:                METHOD_RUN,
			finished_successfully: 0,
			generation_id:         sql.NullInt64{Int64: int64(row.generation_id), Valid: true},
			settings_id:           row.settings_id,
			location:              location,
			created:               uint64(now),
		}

		tt_row.NextLaunchTs.Valid = true
		tt_row.NextLaunchTs.Int64 = now

		if row.jobs_generated_ts.Valid || row.init_jobs_ts.Valid {
			if have_finish_jobs && !row.finish_jobs_ts.Valid {
				set_finish_jobs = append(set_finish_jobs, job_info_key)

				tt_row.JobData = `"finishJobs"`
				tt_row.method = METHOD_FINISH_JOBS
				tt_row.default_retry = settings.retry_job

				add_to_timetable = append(add_to_timetable, tt_row)
			} else {
				prepare_next_generation = append(prepare_next_generation, NextGenParams{Location: job_info_key, JobInfo: row})
			}

			continue
		} else if row.next_generate_job_ts.Int64 > now {
			continue
		}

		if settings.jobs.Type == JOBS_TYPE_CUSTOM {
			set_init_jobs = append(set_init_jobs, job_info_key)

			tt_row.JobData = `"initJobs"`
			tt_row.method = METHOD_INIT_JOBS
			tt_row.default_retry = uint32(settings.retry.Int64)

			add_to_timetable = append(add_to_timetable, tt_row)
			continue
		}

		jobs, mjlErr := makeJobsList(settings.jobs, settings.instance_count, className)
		if mjlErr != nil {
			log.Warningf("Error generating jobs for %+v with instance_count=%d and jobs=%s: %s", className, settings.instance_count, settings.jobs, mjlErr.Error())
			continue
		}

		for _, job := range jobs {
			tt_row_copy := new(TimetableEntry)
			*tt_row_copy = *tt_row
			tt_row_copy.JobData = job
			add_to_timetable = append(add_to_timetable, tt_row_copy)
		}

		set_jobs_generated_js = append(set_jobs_generated_js, job_info_key)
	}

	if err = addJobInfo(tx, add_job_info); err != nil {
		return
	}

	if err = setFinishJobsTs(tx, className, set_finish_jobs); err != nil {
		return
	}

	if err = batchPrepareNextGeneration(tx, have_finish_jobs, className, prepare_next_generation, settings); err != nil {
		return
	}

	if err = setInitJobsTs(tx, className, set_init_jobs); err != nil {
		return
	}

	if err = setJobsGeneratedTs(tx, className, set_jobs_generated_js); err != nil {
		return
	}

	if err = addToTimetable(tx, add_to_timetable); err != nil {
		return
	}

	return
}

func doCycle() bool {
	var (
		jiRows         map[string]map[string]*JobInfoEntry
		scripts        map[string]*ScriptEntry
		flags          map[string]*FlagEntry
		scriptsRusage  map[string]*ScriptRusageEntry
		classLocTTRows map[string]map[string][]*TimetableEntry
	)

	unifiedStartTs := time.Now().UnixNano()

	startTs := time.Now().UnixNano()
	err := loadFullState(
		&LoadStateFunc{name: "Scripts", fun: func() (err error) { scripts, err = getGroupedScriptsForPlatform(); return }},
		&LoadStateFunc{name: "JobInfo", fun: func() (err error) { jiRows, err = getGroupedJobInfo(); return }},
		&LoadStateFunc{name: "Flags", fun: func() (err error) { flags, err = getFlags(); return }},
		&LoadStateFunc{name: "ScriptsRusage", fun: func() (err error) { scriptsRusage, err = getScriptRusageStats(); return }},
		&LoadStateFunc{name: "ScriptTimetable", fun: func() (err error) { classLocTTRows, err = selectTimetable(); return }})

	if err != nil {
		log.Errorf("Failed to select state in doCycle: %s", err.Error())
		return false
	}

	log.Debugf("Loaded for %.5f sec", float64(time.Now().UnixNano()-startTs)/1e9)

	startTs = time.Now().UnixNano()
	err = loadSettingsFromRows(jiRows, scripts)
	if err != nil {
		log.Errorf("Could not load settings from rows: %s", err.Error())
		return false
	}

	func() {
		allSettingsMutex.Lock()
		defer allSettingsMutex.Unlock()

		for _, row := range scripts {
			row.settings = allSettings[row.settings_id]
		}
	}()

	scriptsMap.Lock()
	scriptsMap.v = scripts
	scriptsMap.Unlock()

	log.Debugf("  Selected %d rows from flags", len(flags))
	log.Debugf("  Selected %d rows from scripts rusage", len(scriptsRusage))
	log.Debugf("Load settings for %.5f sec", float64(time.Now().UnixNano()-startTs)/1e9)

	startTs = time.Now().UnixNano()

	// We should not try to generate jobs for scripts that are not present in Script table
	// But we should not forget settings (e.g. last generation_id) for that script
	for class_name := range jiRows {
		if _, ok := scripts[class_name]; !ok {
			delete(jiRows, class_name)
		}
	}

	log.Debugf("Selected all for %.5f sec", float64(time.Now().UnixNano()-unifiedStartTs)/1e9)

	startTs = time.Now().UnixNano()
	updateLoadEstimates()

	log.Debugf("Load estimates updated for %.5f sec", float64(time.Now().UnixNano()-startTs)/1e9)
	func() {
		rusageInfo.Lock()
		defer rusageInfo.Unlock()
		log.Debugf("Group hosts: %+v", rusageInfo.groupHosts)
	}()

	startTs = time.Now().UnixNano()

	failedLocationsMutex.Lock()
	failedLocations = make(map[string]bool)
	failedLocationsMutex.Unlock()

	success := true

	if len(scripts) > 0 {
		throttle.setIntervalCh <- time.Second / time.Duration(len(scripts))
	}

	trigger(throttle.c, "throttle, start of cycle")

	for className, script := range scripts {
		<-throttle.c

		tx := new(db.LazyTrx)
		err := tx.Begin()
		if err != nil {
			log.Errorf("Could not start transaction in job generate: %s", err.Error())
			success = false
			continue
		}

		have := make(map[string]bool)
		locTtRows := classLocTTRows[className]
		if locTtRows != nil {
			for rawLoc, v := range locTtRows {
				loc, err := getLocationIdx(script.settings.location_type, rawLoc)
				if err != nil {
					log.Warningf("Broken settings for class %s: %s", className, err.Error())
					loc = rawLoc
				}
				if len(v) > 0 {
					have[loc] = true
				}
			}
		}

		add_to_timetable, err := generateJobs(tx, className, script.settings, jiRows[className], have, flags[className])

		if err != nil {
			log.Errorf("Could generate jobs for class %s: %s", className, err.Error())
			tx.Rollback()
			success = false
			continue
		}

		err = tx.Commit()
		if err != nil {
			log.Errorf("Could not commit generate jobs for class %s: %s", className, err.Error())
			success = false
			continue
		}

		per_location := make(map[string][]*TimetableEntry)

		for _, row := range add_to_timetable {
			allSettingsMutex.Lock()
			row.settings = allSettings[row.settings_id]
			allSettingsMutex.Unlock()

			if row.settings == nil {
				log.Warningf("Internal inconsistency error: Invalid settings for generated row: %+v", row)
				continue
			}

			key := DEFAULT_LOCATION_IDX
			if row.settings.location_type == LOCATION_TYPE_EACH {
				key = row.location
			}

			if _, ok := per_location[key]; !ok {
				per_location[key] = make([]*TimetableEntry, 0)
			}

			per_location[key] = append(per_location[key], row)
		}

		for location, rows := range per_location {
			notifyAboutNewTTRows(className, location, rows, true)
		}
	}

	notifyForFullTTSelect(classLocTTRows, true)

	log.Debugf("Processed %d classes for %.5f sec", len(scripts), float64(time.Now().UnixNano()-startTs)/1e9)
	log.Debugf("Total %.5f sec", float64(time.Now().UnixNano()-unifiedStartTs)/1e9)

	return success
}

func GenerateJobsCycle() {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("Could not get hostname: %s", err.Error())
	}

	log.Print("Initial select from RunQueue and starting launcher goroutines")
	if err := selectRQAndNotify(); err != nil {
		log.Fatalf("Could not do initial select run queue: %s", err.Error())
	}

	log.Print("Initial select from Timetable")
	ttRows, err := selectTimetable()
	if err != nil {
		log.Fatalf("Could not do initial select timetable: %s", err.Error())
	}

	log.Print("Starting jobgen goroutines")
	if err := notifyForFullTTSelect(ttRows, false); err != nil {
		log.Fatalf("Could notify about timetable: %s", err.Error())
	}

	for {
		res, err := db.LockCycle(getLockName(), hostname)
		if err != nil || !res {
			if err == nil {
				log.Println("Could not get lock, another host holds it? Retrying in 10 seconds")
			} else {
				log.Warningf("Could not get lock, got DB error: ", err.Error())
			}

			time.Sleep(time.Second * 10)
			continue
		}

		// timer := pinba.TimerStart(map[string]string{"group": "jobgenerator"})
		startTs := time.Now().UnixNano()

		db.LogCycleStart(CYCLE_CLASS_NAME, hostname, 0)
		log.Debug("Cycle started")
		success := doCycle()
		log.Debug("Cycle finished")
		successInt := 1
		if !success {
			successInt = 0
		}
		db.LogCycleStop(CYCLE_CLASS_NAME, hostname, 0, successInt)

		passedMs := int64((time.Now().UnixNano() - startTs) / 1e6)

		if passedMs < cycleMs {
			time.Sleep(time.Duration(cycleMs-passedMs) * time.Millisecond)
		}
	}
}

func getAutoIncrementIncrement() int64 {
	rows, err := db.Query("SHOW VARIABLES LIKE 'auto_increment_increment'")
	if err != nil {
		log.Fatal("Could not get auto_increment_increment variable from DB: " + err.Error())
	}

	defer rows.Close()

	var varName string
	var res int64

	for rows.Next() {
		if err := rows.Scan(&varName, &res); err != nil {
			log.Fatal("Could not fetch results for auto_increment_increment variable: " + err.Error())
		}

		if varName == "auto_increment_increment" {
			return res
		}
	}

	return 1 // default value if no special variable is set
}

func throttleThread() {
	interv := time.Millisecond

	for {
		select {
		case <-time.After(interv):
			trigger(throttle.c, "throttle")
		case interv = <-throttle.setIntervalCh:
		}
	}
}

func Setup(config common.FullConfig) {
	isDevelServer = config.GetIsDevel()

	dispatchThreads.v = make(map[string]map[string]*DispatcherData)
	launcherThreads.v = make(map[string]*LauncherData)
	killerThreads.v = make(map[string]map[string]bool)

	throttle.c = make(chan bool, THROTTLE_CHAN_CAPACITY)
	throttle.setIntervalCh = make(chan time.Duration, 1)
	go throttleThread()

	rusageInfo.groupsMaxParrots = make(map[string]uint64)
	rusageInfo.groupHosts = make(map[string][]string)
	rusageInfo.hostsInfo = make(map[string]*ServerInfo)
	rusageInfo.loadEstimate = make(map[string]*ScriptRusageEntry)
	rusageInfo.timetableRusage = make(map[uint64]*ScriptRusageEntry)
	rusageInfo.groupIdx = make(map[string]uint64)

	def := config.GetDefault()

	defaultParasiteMemory = def.GetParasiteMemory()
	defaultMinIdleCpu = def.GetMinIdleCpu()
	defaultMinMemory = def.GetMinMemory()
	defaultMinMemoryRatio = def.GetMinMemoryRatio()
	defaultMaxMemory = def.GetMaxMemory()
	defaultRusage = def.GetRusage()

	cycleMs = config.GetCycleMs()
	ttReloadIntervalMs = config.GetFullTimetableReloadIntervalMs()
	autoIncrementIncrement = getAutoIncrementIncrement()

	launcherConf := config.GetLauncher()
	hostSuffix = launcherConf.GetHostSuffix()
	baseDir = launcherConf.GetBaseDir()
	if launcherConf.DeveloperDir != nil {
		haveDeveloper = true
		developerDir = launcherConf.GetDeveloperDir()
		log.Printf("We have developer dir: %s", developerDir)
	}

	log.Printf("Updating hosts")

	updateHosts()

	log.Printf("Launching update hosts thread")

	go updateHostsThread()

	log.Printf("Clearing old heartbeats")

	if err := clearOldHeartbeats(); err != nil {
		log.Fatalf("Could not clear old heartbeats: %s", err.Error())
	}

	log.Printf("Launching periodic run queue select thread")

	go runqueuePeriodicSelectThread()
	go forceCheckDeletedThread()
}
