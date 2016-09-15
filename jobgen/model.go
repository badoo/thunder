package jobgen

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"badoo/_packages/log"
	"github.com/badoo/thunder/common"
	"github.com/badoo/thunder/db"
	"sort"
	"sync"
)

var (
	allSettings      = make(map[uint64]*ScriptSettings)
	allSettingsMutex sync.Mutex
)

const (
	TABLE_ACTION_LOG          = "ActionLog"
	TABLE_SCRIPT              = "Script"
	TABLE_ACTION_SETTINGS     = "ActionScriptSettings"
	TABLE_SCRIPT_SETTINGS     = "ScriptSettings"
	TABLE_SCRIPT_JOB_INFO     = "ScriptJobInfo"
	TABLE_SCRIPT_JOB_RESULT   = "ScriptJobResult"
	TABLE_SCRIPT_FAIL_INFO    = "ScriptFailInfo"
	TABLE_SCRIPT_RUSAGE_STATS = "ScriptRusageStats"
	TABLE_SCRIPT_TIMETABLE    = "ScriptTimetable"
	TABLE_SCRIPT_TAG          = "ScriptTag"
	TABLE_SCRIPT_FLAGS        = "ScriptFlags"
	TABLE_SERVER              = "Server"
	TABLE_SERVER_GROUP        = "ServerGroup"
	TABLE_RUN_QUEUE           = "RunQueue"
)

//language=SQL
const (
	QUERY_SELECT_FROM_TIMETABLE = `SELECT
		id, generation_id, class_name, ` + "`repeat`" + `, retry_count, default_retry, job_data, method,
		location, UNIX_TIMESTAMP(finished_ts), finished_successfully, finish_count, UNIX_TIMESTAMP(next_launch_ts),
		UNIX_TIMESTAMP(added_to_queue_ts), token, settings_id, UNIX_TIMESTAMP(created)
		FROM ` + TABLE_SCRIPT_TIMETABLE + ` #add_where#`

	QUERY_GET_RUNQUEUE = `SELECT
		id, class_name, timetable_id, generation_id, hostname, hostname_idx, job_data, method, run_status,
		UNIX_TIMESTAMP(created), UNIX_TIMESTAMP(waiting_ts), UNIX_TIMESTAMP(should_init_ts), init_attempts,
		UNIX_TIMESTAMP(init_ts), UNIX_TIMESTAMP(running_ts), UNIX_TIMESTAMP(max_finished_ts), UNIX_TIMESTAMP(finished_ts),
		stopped_employee_id, token, retry_attempt, settings_id
      FROM ` + TABLE_RUN_QUEUE + ` #where#`

	QUERY_SIMPLE_GET_SCRIPTS_FOR_PLATFORM = "SELECT class_name, settings_id FROM " + TABLE_SCRIPT

	QUERY_GET_NEW_SETTINGS = `SELECT id, class_name, instance_count, max_time, jobs,
		next_ts_callback, ` + "`repeat`" + `, retry, ttl, repeat_job, retry_job, location, location_type, developer, max_retries,
		profiling_enabled, debug_enabled, named_params, UNIX_TIMESTAMP(created)
		FROM ` + TABLE_SCRIPT_SETTINGS + `
		WHERE id IN(#new_settings_ids#)`

	QUERY_GET_JOB_INFO = `SELECT generation_id, class_name, location,
		UNIX_TIMESTAMP(init_jobs_ts), UNIX_TIMESTAMP(jobs_generated_ts), UNIX_TIMESTAMP(finish_jobs_ts), UNIX_TIMESTAMP(next_generate_job_ts),
		settings_id
		FROM ` + TABLE_SCRIPT_JOB_INFO

	QUERY_GET_FLAGS = `SELECT class_name,
		UNIX_TIMESTAMP(run_requested_ts), UNIX_TIMESTAMP(run_accepted_ts), UNIX_TIMESTAMP(pause_requested_ts), UNIX_TIMESTAMP(kill_requested_ts),
		kill_request_employee_id, UNIX_TIMESTAMP(run_queue_killed_ts), UNIX_TIMESTAMP(killed_ts), UNIX_TIMESTAMP(paused_ts)
		FROM ` + TABLE_SCRIPT_FLAGS

	QUERY_GET_SCRIPTS_RUSAGE_STATS = `SELECT class_name, real_time, sys_time, user_time, max_memory
		FROM ` + TABLE_SCRIPT_RUSAGE_STATS

	QUERY_GET_AVAILABLE_HOSTS = `SELECT
            hostname,
            ` + "`group`" + `,
            FLOOR(cpu_idle * cpu_parrots_per_core * cpu_cores / 100) AS cpu_idle_parrots,
            cpu_parrots_per_core,
            ROUND(cpu_idle * cpu_cores / 100, 2) AS cpu_idle_cores,
            cpu_cores,
            cpu_parasite,
            mem_total,
            mem_free,
            mem_cached,
            mem_parasite,
            swap_used,
            min_memory,
            min_memory_ratio
        FROM ` + TABLE_SERVER + `
        WHERE phproxyd_heartbeat_ts > NOW() - INTERVAL 15 SECOND AND disabled_ts IS NULL
        ORDER BY cpu_idle_parrots DESC`

	// Update queries
	QUERY_INSERT_INTO_TIMETABLE = `INSERT INTO ` + TABLE_SCRIPT_TIMETABLE + `
		(class_name, default_retry, ` + "`repeat`" + `, method, finished_successfully, generation_id, settings_id, location, job_data, shard_id, created, next_launch_ts)
		VALUES #values#`

	QUERY_INSERT_INTO_JOB_INFO = `INSERT INTO ` + TABLE_SCRIPT_JOB_INFO + ` #fields#
		VALUES #values#`

	QUERY_SET_JOBS_GENERATED_TS = `UPDATE ` + TABLE_SCRIPT_JOB_INFO + `
        SET jobs_generated_ts = NOW()
        WHERE class_name = '#class_name#' AND location IN(#locations#)`

	QUERY_CLEAR_JOB_RESULTS = `DELETE FROM ` + TABLE_SCRIPT_JOB_RESULT + `
        WHERE class_name = '#class_name#'`

	QUERY_CLEAR_JOB_RESULTS_FOR_LOCATIONS = `DELETE FROM ` + TABLE_SCRIPT_JOB_RESULT + `
        WHERE class_name = '#class_name#' AND location IN(#locations#)`

	QUERY_SET_NEXT_GENERATE_JOB_TS = `UPDATE ` + TABLE_SCRIPT_JOB_INFO + `
        SET
            next_generate_job_ts = FROM_UNIXTIME(#next_generate_job_ts#),
            jobs_generated_ts = NULL, jobs_finished_ts = NULL, init_jobs_ts = NULL, finish_jobs_ts = NULL,
            generation_id = generation_id + 1, settings_id = #settings_id#
        WHERE class_name = '#class_name#'`

	QUERY_BATCH_SET_NEXT_GENERATE_JOB_TS = `INSERT INTO ` + TABLE_SCRIPT_JOB_INFO + `
		#fields#
		VALUES #values#
		ON DUPLICATE KEY UPDATE
			next_generate_job_ts = VALUES(next_generate_job_ts), jobs_generated_ts = VALUES(jobs_generated_ts),
			jobs_finished_ts = VALUES(jobs_finished_ts), init_jobs_ts = VALUES(init_jobs_ts),
			finish_jobs_ts = VALUES(finish_jobs_ts), generation_id = generation_id + 1,
			settings_id = VALUES(settings_id)`

	QUERY_SET_INIT_JOBS_TS = `UPDATE ` + TABLE_SCRIPT_JOB_INFO + `
        SET init_jobs_ts = NOW()
        WHERE class_name = '#class_name#' AND location IN(#locations#)`

	QUERY_SET_FINISH_JOBS_TS = `UPDATE ` + TABLE_SCRIPT_JOB_INFO + `
        SET finish_jobs_ts = NOW()
        WHERE class_name = '#class_name#' AND location IN(#locations#)`

	QUERY_SET_MAX_FINISHED_TS_NOW = `UPDATE ` + TABLE_RUN_QUEUE + `
        SET max_finished_ts = FROM_UNIXTIME(#ts#), stopped_employee_id = #employee_id#
        WHERE class_name = '#class_name#'`

	QUERY_INSERT_INTO_RUN_QUEUE = "INSERT INTO " + TABLE_RUN_QUEUE + "#fields# VALUES#values#"

	QUERY_LOG_ADD_TO_QUEUE = "UPDATE " + TABLE_SCRIPT_TIMETABLE + " SET added_to_queue_ts = NOW() WHERE id IN(#ids#) AND added_to_queue_ts IS NULL"

	QUERY_UPDATE_TIMETABLE_STATUS = "UPDATE " + TABLE_SCRIPT_TIMETABLE + ` SET
		finished_ts = FROM_UNIXTIME(#finished_ts#), next_launch_ts = FROM_UNIXTIME(#next_launch_ts#), added_to_queue_ts = FROM_UNIXTIME(#added_to_queue_ts#),
		retry_count = #retry_count#, finish_count = #finish_count#, finished_successfully = #finished_successfully#
		WHERE id = #id# #add_where#`

	QUERY_DELETE_FROM_TIMETABLE = "DELETE FROM " + TABLE_SCRIPT_TIMETABLE + " WHERE id IN(#ids#) #add_where#"

	QUERY_RESET_RUN_REQUEST = `UPDATE ` + TABLE_SCRIPT_FLAGS + `
        SET run_requested_ts = NULL, run_accepted_ts = NULL
        WHERE class_name = '#class_name#'`

	QUERY_SET_RUN_ACCEPTED = `UPDATE ` + TABLE_SCRIPT_FLAGS + `
        SET run_accepted_ts = NOW()
        WHERE class_name = '#class_name#'`

	QUERY_DELETE_FROM_QUEUE = "DELETE FROM " + TABLE_RUN_QUEUE + " WHERE id IN(#ids#) AND run_status = '#status#'"

	QUERY_UPDATE_RUN_STATUS = "UPDATE " + TABLE_RUN_QUEUE + `
		SET run_status = '#status#', #status#_ts = NOW()
		WHERE id = #id# AND run_status = '#prev_status#'`

	QUERY_UPDATE_RUN_STATUS_INIT = "UPDATE " + TABLE_RUN_QUEUE + `
		SET run_status = 'Init', init_ts = NOW(), max_finished_ts = created + INTERVAL #max_time# SECOND
		WHERE id = #id#`

	QUERY_QUERY_CLEAR_OLD_HEARTBEATS = `DELETE FROM ` + TABLE_RUN_QUEUE + `
		WHERE class_name = '#class_name#'
		AND run_status = 'Waiting' AND created < NOW() - INTERVAL 30 SECOND`

	QUERY_CHECK_TT_IDS = "SELECT id FROM " + TABLE_SCRIPT_TIMETABLE + " WHERE id IN(#ids#)"
)

var errorNoRow = errors.New("No such row")

func getRunningInfo(run_id uint64) (*RunQueueEntry, error) {
	rows, err := db.Query(QUERY_GET_RUNQUEUE, "where", &db.RawData{Data: fmt.Sprintf("WHERE id = %d", run_id)})
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		rq_entry, err := scanRqEntry(rows)
		if err != nil {
			return nil, err
		}

		return rq_entry, nil
	}

	return nil, errorNoRow
}

func getRunningInfos(run_ids []uint64) ([]*RunQueueEntry, error) {
	res := make([]*RunQueueEntry, 0, len(run_ids))
	if len(run_ids) == 0 {
		return res, nil
	}
	rows, err := db.Query(QUERY_GET_RUNQUEUE, "where", &db.RawData{Data: fmt.Sprintf("WHERE id IN (%s)", db.INUint64(run_ids).Data)})
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		rq_entry, err := scanRqEntry(rows)
		if err != nil {
			return nil, err
		}

		res = append(res, rq_entry)
	}

	return res, nil
}

func deleteFromRunQueue(tx *db.LazyTrx, ids []uint64, prevStatus string) error {
	if len(ids) == 0 {
		return nil
	}

	res, err := tx.Exec(
		QUERY_DELETE_FROM_QUEUE,
		"ids", db.INUint64(ids),
		"status", prevStatus)
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff != int64(len(ids)) {
		return fmt.Errorf("deleteFromRunQueue failed aff:%d expected:%d ids:%+v prevStatus:%s", aff, len(ids), ids, prevStatus)
	}
	return nil
}

func updateRunStatus(tx *db.LazyTrx, run_id uint64, status, prevStatus string) error {
	res, err := tx.Exec(
		QUERY_UPDATE_RUN_STATUS,
		"status", status,
		"prev_status", prevStatus,
		"id", run_id)

	if err != nil {
		return err
	}

	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if aff != 1 {
		return fmt.Errorf("Previous status mismatch for rq row id=%d: tried %s -> %s", run_id, prevStatus, status)
	}

	return nil
}

func setRunStatusToInit(tx *db.LazyTrx, run_id uint64, max_time int) (err error) {
	_, err = tx.Exec(
		QUERY_UPDATE_RUN_STATUS_INIT,
		"max_time", max_time,
		"id", run_id)
	return
}

func getLockName() string {
	return "jobgen"
}

func scanTimetableEntry(rows *sql.Rows) (*TimetableEntry, error) {
	tt_entry := new(TimetableEntry)
	err := rows.Scan(
		&tt_entry.id,
		&tt_entry.generation_id,
		&tt_entry.class_name,
		&tt_entry.repeat,
		&tt_entry.retry_count,
		&tt_entry.default_retry,
		&tt_entry.JobData,
		&tt_entry.method,
		&tt_entry.location,
		&tt_entry.finished_ts,
		&tt_entry.finished_successfully,
		&tt_entry.finish_count,
		&tt_entry.NextLaunchTs,
		&tt_entry.added_to_queue_ts,
		&tt_entry.token,
		&tt_entry.settings_id,
		&tt_entry.created)

	if err != nil {
		return nil, err
	}
	return tt_entry, nil
}

func selectTimetable() (map[string]map[string][]*TimetableEntry, error) {
	timetableRows := make(map[string]map[string][]*TimetableEntry)

	rows, err := db.Query(QUERY_SELECT_FROM_TIMETABLE, "add_where", "")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		tt_entry, err := scanTimetableEntry(rows)
		if err != nil {
			return nil, err
		}

		perLoc, ok := timetableRows[tt_entry.class_name]
		if !ok {
			perLoc = make(map[string][]*TimetableEntry)
			timetableRows[tt_entry.class_name] = perLoc
		}

		if _, ok := perLoc[tt_entry.location]; !ok {
			perLoc[tt_entry.location] = make([]*TimetableEntry, 0)
		}

		perLoc[tt_entry.location] = append(perLoc[tt_entry.location], tt_entry)
	}

	return timetableRows, nil
}

func selectTimetableByIds(ids []uint64) ([]*TimetableEntry, error) {
	timetableRows := make([]*TimetableEntry, 0, len(ids))
	if len(ids) == 0 {
		return timetableRows, nil
	}

	rows, err := db.Query(QUERY_SELECT_FROM_TIMETABLE, "add_where", &db.RawData{Data: fmt.Sprintf("WHERE id IN (%s)", db.INUint64(ids).Data)})
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		tt_entry, err := scanTimetableEntry(rows)
		if err != nil {
			return nil, err
		}
		timetableRows = append(timetableRows, tt_entry)
	}

	return timetableRows, nil
}

func setKilledFlag(tx *db.LazyTrx, className string) (err error) {
	_, err = tx.Exec(
		`UPDATE `+TABLE_SCRIPT_FLAGS+` SET kill_requested_ts = NULL, killed_ts = NOW() WHERE class_name = '#class_name#'`,
		"class_name", className)
	return
}

func setPausedFlag(tx *db.LazyTrx, className string) (err error) {
	_, err = tx.Exec(
		`UPDATE `+TABLE_SCRIPT_FLAGS+` SET paused_ts = NOW() WHERE class_name = '#class_name#'`,
		"class_name", className)
	return
}

func prepareNextGeneration(tx *db.LazyTrx, have_finish_jobs bool, className string, settings *ScriptSettings) (err error) {
	if have_finish_jobs {
		if err = clearJobsResults(tx, className); err != nil {
			return
		}
	}

	if err = setNextGenerateJobTs(tx, className, settings); err != nil {
		return
	}

	return
}

type NextGenParams struct {
	Location string
	JobInfo  *JobInfoEntry
}

func batchPrepareNextGeneration(tx *db.LazyTrx, have_finish_jobs bool, className string, params []NextGenParams, settings *ScriptSettings) (err error) {
	if have_finish_jobs {
		locations := make([]string, 0, len(params))
		for _, par := range params {
			locations = append(locations, par.Location)
		}
		if err = clearJobsResultsForLocations(tx, className, locations); err != nil {
			return
		}
	}

	if err = batchSetNextGenerateJobTs(tx, className, params, settings); err != nil {
		return
	}

	return
}

func batchSetNextGenerateJobTs(tx *db.LazyTrx, className string, params []NextGenParams, settings *ScriptSettings) (err error) {
	if len(params) == 0 {
		return
	}

	fields := `(class_name, location, next_generate_job_ts, jobs_generated_ts, jobs_finished_ts, init_jobs_ts, finish_jobs_ts, generation_id, settings_id)`
	values := make([]string, 0, len(params))

	for _, par := range params {
		firstRun := !par.JobInfo.jobs_generated_ts.Valid
		var prevTs, nextTs uint64

		if !firstRun {
			prevTs = uint64(par.JobInfo.jobs_generated_ts.Int64)
		}

		nextTs = getNextJobGenerateTs(className, firstRun, prevTs, settings)

		values = append(values, fmt.Sprintf("('%s', '%s', FROM_UNIXTIME(%d), NULL, NULL, NULL, NULL, 1, %d)",
			db.EscapeString(className), db.EscapeString(par.Location), nextTs, settings.id))
	}

	_, err = tx.Exec(
		QUERY_BATCH_SET_NEXT_GENERATE_JOB_TS,
		"fields", fields,
		"values", &db.RawData{Data: strings.Join(values, ", ")})
	return
}

func setNextGenerateJobTs(tx *db.LazyTrx, className string, settings *ScriptSettings) (err error) {
	nextTs := getNextJobGenerateTs(className, true, 0, settings)

	_, err = tx.Exec(
		QUERY_SET_NEXT_GENERATE_JOB_TS,
		"class_name", className,
		"settings_id", settings.id,
		"next_generate_job_ts", nextTs)
	return
}

func setMaxFinishedTs(tx *db.LazyTrx, className string, employeeId int64, ts int64) (err error) {
	_, err = tx.Exec(
		QUERY_SET_MAX_FINISHED_TS_NOW,
		"class_name", className,
		"employee_id", employeeId,
		"ts", ts)
	return
}

func clearJobsResults(tx *db.LazyTrx, className string) (err error) {
	_, err = tx.Exec(
		QUERY_CLEAR_JOB_RESULTS,
		"class_name", className)
	return
}

func clearJobsResultsForLocations(tx *db.LazyTrx, className string, locations []string) (err error) {
	if len(locations) == 0 {
		return
	}

	_, err = tx.Exec(
		QUERY_CLEAR_JOB_RESULTS_FOR_LOCATIONS,
		"class_name", className,
		"locations", db.INStr(locations))
	return
}

/**
 * Whether or not current generation has properly finished (with finish_jobs applied).
 *
 * If location_type is not 'any', then just isset($timetable[$class_name]) is done
 * which does will not allow finishJobs to run
 *
 * @param $class_name
 * @param array $timetable   Timetable (
 * @param array $job_info    Job info array(location_idx => ...)
 * @param array $info        Script settings
 * @return bool
 */
func generationFinished(className string, haveTTRows map[string]bool, jiRows map[string]*JobInfoEntry, settings *ScriptSettings) bool {
	if haveTTRows != nil {
		return false
	}

	if settings.location_type != LOCATION_TYPE_ANY {
		return true
	}

	// Haven't tried to generate a single generation yet
	if _, ok := jiRows[DEFAULT_LOCATION_IDX]; !ok {
		return true
	}

	ji := jiRows[DEFAULT_LOCATION_IDX]

	// Haven't yet started generating jobs for new generation
	if !ji.jobs_generated_ts.Valid {
		return true
	}

	// No finish_jobs, so isset(timetable[class_name]) is enough
	if !settings.jobs.Have_finish_jobs {
		return true
	}

	// Finish jobs must have started and then disappeared before timetable[class_name] would be empty
	return ji.finish_jobs_ts.Valid
}

func addJobInfo(tx *db.LazyTrx, rows []*JobInfoEntry) (err error) {
	if len(rows) == 0 {
		return
	}

	fields := `(class_name, location, next_generate_job_ts, settings_id)`
	values := make([]string, 0, len(rows))

	for _, row := range rows {
		values = append(values, fmt.Sprintf(
			"('%s', '%s', FROM_UNIXTIME(%d), %d)",
			db.EscapeString(row.class_name), db.EscapeString(row.location), row.next_generate_job_ts.Int64, row.settings_id))
	}

	_, err = tx.Exec(
		QUERY_INSERT_INTO_JOB_INFO,
		"fields", fields,
		"values", &db.RawData{Data: strings.Join(values, ",")})
	return
}

func setFinishJobsTs(tx *db.LazyTrx, className string, locations []string) (err error) {
	if len(locations) == 0 {
		return
	}

	_, err = tx.Exec(
		QUERY_SET_FINISH_JOBS_TS,
		"class_name", className,
		"locations", db.INStr(locations))
	return
}

func setInitJobsTs(tx *db.LazyTrx, className string, locations []string) (err error) {
	if len(locations) == 0 {
		return
	}

	_, err = tx.Exec(
		QUERY_SET_INIT_JOBS_TS,
		"class_name", className,
		"locations", db.INStr(locations))
	return
}

func setJobsGeneratedTs(tx *db.LazyTrx, className string, locations []string) (err error) {
	if len(locations) == 0 {
		return
	}

	_, err = tx.Exec(
		QUERY_SET_JOBS_GENERATED_TS,
		"class_name", className,
		"locations", db.INStr(locations))
	return
}

func addToQueueAndDeleteExpired(tx *db.LazyTrx, rows []*RunQueueEntry, toDelete []*TimetableEntry) error {
	if len(rows) > 0 {
		values := make([]string, 0, len(rows))
		ttIds := make([]uint64, 0, len(rows))

		fields := `(class_name, timetable_id, generation_id, hostname, hostname_idx, job_data, method, created, run_status, waiting_ts, should_init_ts, token, retry_attempt, settings_id)`

		for _, row := range rows {
			ttIds = append(ttIds, uint64(row.timetable_id.Int64))

			val := fmt.Sprint(
				"('"+db.EscapeString(row.ClassName)+"',",
				db.QNullInt64(row.timetable_id).Data, ",",
				row.generation_id.Int64, ",",
				"'"+db.EscapeString(row.hostname)+"',",
				row.hostname_idx, ",",
				"'"+db.EscapeString(row.JobData)+"',",
				"'"+db.EscapeString(row.method)+"',",
				"FROM_UNIXTIME(", row.created.Int64, "),",
				"'"+db.EscapeString(row.RunStatus)+"',",
				"FROM_UNIXTIME(", row.waiting_ts.Int64, "),",
				"FROM_UNIXTIME(", row.should_init_ts.Int64, "),",
				"'"+db.EscapeString(row.token), "',",
				row.retry_attempt, ",",
				row.settings_id, ")")

			values = append(values, val)
		}

		res, err := tx.Exec(QUERY_INSERT_INTO_RUN_QUEUE, "fields", fields, "values", &db.RawData{Data: strings.Join(values, ",")})
		if err != nil {
			return err
		}

		insId, err := res.LastInsertId()
		if err != nil {
			return err
		}

		for _, row := range rows {
			row.Id = uint64(insId)
			insId += autoIncrementIncrement
		}

		sort.Sort(common.UInt64Slice(ttIds))

		res, err = tx.Exec(QUERY_LOG_ADD_TO_QUEUE, "ids", db.INUint64(ttIds))
		if err != nil {
			return err
		}
		aff, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if aff != int64(len(ttIds)) {
			return fmt.Errorf("update ur cache bro aff: %d ttIds:%+v", aff, ttIds)
		}
	}

	if len(toDelete) > 0 {
		ttIds := make([]uint64, 0, len(toDelete))
		for _, row := range toDelete {
			ttIds = append(ttIds, row.id)
		}

		sort.Sort(common.UInt64Slice(ttIds))

		res, err := tx.Exec(QUERY_DELETE_FROM_TIMETABLE, "ids", db.INUint64(ttIds), "add_where", " AND added_to_queue_ts IS NULL")
		if err != nil {
			return err
		}
		aff, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if aff != int64(len(ttIds)) {
			return fmt.Errorf("addToQueueAndDeleteExpired unexpected ttIds deleted count:%d instead of %d ids:%+v", aff, len(ttIds), ttIds)
		}
	}

	return nil
}

// get existing ids from db for provided checkIds list
func getExistingTTIds(checkIds []uint64) (map[uint64]bool, error) {
	res := make(map[uint64]bool, len(checkIds))
	if len(checkIds) == 0 {
		return res, nil
	}

	rows, err := db.Query(QUERY_CHECK_TT_IDS, "ids", db.INUint64(checkIds))
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	var id uint64
	for rows.Next() {
		err = rows.Scan(&id)

		if err != nil {
			return nil, err
		}

		res[id] = true
	}

	return res, nil
}

func logTTFinish(tx *db.LazyTrx, row *TimetableEntry, haveOldFinishCount bool, oldFinishCount uint) error {
	var addedToQueueWhere, addedFinishCountWhere string
	if row.added_to_queue_ts.Valid {
		addedToQueueWhere = "AND added_to_queue_ts IS NULL"
	} else {
		addedToQueueWhere = "AND added_to_queue_ts IS NOT NULL"
	}

	if haveOldFinishCount {
		addedFinishCountWhere = fmt.Sprintf("AND finish_count = %d", oldFinishCount)
	} else {
		addedFinishCountWhere = fmt.Sprintf("AND finish_count = %d", row.finish_count-1)
	}

	res, err := tx.Exec(QUERY_UPDATE_TIMETABLE_STATUS,
		"finished_ts", db.QNullInt64(row.finished_ts),
		"next_launch_ts", db.QNullInt64(row.NextLaunchTs),
		"added_to_queue_ts", db.QNullInt64(row.added_to_queue_ts),
		"finish_count", row.finish_count,
		"retry_count", row.retry_count,
		"finished_successfully", row.finished_successfully,
		"id", row.id,
		"add_where", fmt.Sprintf("%s %s", addedToQueueWhere, addedFinishCountWhere))

	if err != nil {
		return err
	}

	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if aff != 1 {
		return fmt.Errorf("unexpected affected rows = %d for tt_row = %+v", aff, row)
	}

	return nil
}

func deleteAddedFromTimetable(tx *db.LazyTrx, ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}

	res, err := tx.Exec(QUERY_DELETE_FROM_TIMETABLE, "ids", db.INUint64(ids), "add_where", " AND added_to_queue_ts IS NOT NULL")
	if err != nil {
		return err
	}
	aff, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if aff != int64(len(ids)) {
		return fmt.Errorf("deleteAddedFromTimetable unexpected ttIds deleted count:%d instead of %d ids:%+v", aff, len(ids), ids)
	}
	return nil
}

func resetRunRequest(tx *db.LazyTrx, className string) (err error) {
	_, err = tx.Exec(QUERY_RESET_RUN_REQUEST, "class_name", className)
	return
}

func setRunAccepted(tx *db.LazyTrx, className string) (err error) {
	_, err = tx.Exec(QUERY_SET_RUN_ACCEPTED, "class_name", className)
	return
}

// method inserts rows into timetable and sets insert id
func addToTimetable(tx *db.LazyTrx, ttRows []*TimetableEntry) error {
	if len(ttRows) == 0 {
		return nil
	}

	values := make([]string, 0)

	for _, row := range ttRows {
		val := fmt.Sprintf(
			"('%s', %d, %s, '%s', %d, %s, %d, '%s', '%s', %d, FROM_UNIXTIME(%d), FROM_UNIXTIME(%d))",
			db.EscapeString(row.class_name),
			row.default_retry,
			db.QNullInt64(row.repeat).Data,
			row.method,
			row.finished_successfully,
			db.QNullInt64(row.generation_id).Data,
			row.settings_id,
			db.EscapeString(row.location),
			db.EscapeString(row.JobData),
			0,
			row.created,
			row.NextLaunchTs.Int64)

		values = append(values, val)
	}

	res, err := tx.Exec(QUERY_INSERT_INTO_TIMETABLE, "values", &db.RawData{Data: strings.Join(values, ", ")})
	if err != nil {
		return err
	}

	insId, err := res.LastInsertId()
	if err != nil {
		log.Errorf("Could not get insert id even though insert was successfull: %s", err.Error())
		return err
	}

	for _, row := range ttRows {
		row.id = uint64(insId)
		insId += autoIncrementIncrement
	}

	return nil
}

func makeJobsList(jobs Jobs, instanceCount int, className string) (result []string, err error) {
	result = make([]string, 0)

	switch jobs.Type {
	case JOBS_TYPE_INSTANCES:
		for i := 1; i <= instanceCount; i++ {
			result = append(result, strconv.Itoa(i))
		}
	case JOBS_TYPE_RANGE:
		for i := jobs.Min; i <= jobs.Max; i++ {
			result = append(result, strconv.Itoa(i))
		}
	default:
		err = errors.New("Unsupported job type " + jobs.Type + " for class " + className)
	}

	return
}

func scanRqEntry(rows *sql.Rows) (rq_entry *RunQueueEntry, err error) {
	rq_entry = new(RunQueueEntry)
	err = rows.Scan(
		&rq_entry.Id,
		&rq_entry.ClassName,
		&rq_entry.timetable_id,
		&rq_entry.generation_id,
		&rq_entry.hostname,
		&rq_entry.hostname_idx,
		&rq_entry.JobData,
		&rq_entry.method,
		&rq_entry.RunStatus,
		&rq_entry.created,
		&rq_entry.waiting_ts,
		&rq_entry.should_init_ts,
		&rq_entry.init_attempts,
		&rq_entry.init_ts,
		&rq_entry.running_ts,
		&rq_entry.max_finished_ts,
		&rq_entry.finished_ts,
		&rq_entry.stopped_employee_id,
		&rq_entry.token,
		&rq_entry.retry_attempt,
		&rq_entry.settings_id)
	return
}

func SelectRunQueue() (map[string][]*RunQueueEntry, error) {
	rqRows := make(map[string][]*RunQueueEntry, 0)
	rows, err := db.Query(QUERY_GET_RUNQUEUE, "where", "")
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		rq_entry, err := scanRqEntry(rows)
		if err != nil {
			return nil, err
		}

		rqRows[rq_entry.hostname] = append(rqRows[rq_entry.hostname], rq_entry)
	}

	return rqRows, nil
}

func loadNewIds(newIds map[uint64]bool) error {
	if len(newIds) == 0 {
		return nil
	}

	var ids []string

	for id := range newIds {
		ids = append(ids, strconv.FormatUint(id, 10))
	}

	rows, err := db.Query(QUERY_GET_NEW_SETTINGS, "new_settings_ids", strings.Join(ids, ","))
	if err != nil {
		return err
	}

	for rows.Next() {
		entry := new(ScriptSettings)

		var (
			jobsStr           string
			nextTsCallbackStr sql.NullString
		)

		err = rows.Scan(
			&entry.id,
			&entry.class_name,
			&entry.instance_count,
			&entry.max_time,
			&jobsStr,
			&nextTsCallbackStr,
			&entry.repeat,
			&entry.retry,
			&entry.ttl,
			&entry.repeat_job,
			&entry.retry_job,
			&entry.location,
			&entry.location_type,
			&entry.developer,
			&entry.max_retries,
			&entry.profiling_enabled,
			&entry.debug_enabled,
			&entry.named_params,
			&entry.created)

		if err != nil {
			log.Errorf("Invalid settings: %s", err.Error())
			err = nil
			continue
		}

		entry.jobs, err = parseJobs(jobsStr)
		if err != nil {
			log.Errorf("Could not parse Jobs for %s #%d: %s", entry.class_name, entry.id, err.Error())
			err = nil
			continue
		}

		if nextTsCallbackStr.Valid {
			entry.have_next_ts_callback = true
			entry.next_ts_callback, err = parseNextTsCallback(nextTsCallbackStr.String)
			if err != nil {
				log.Errorf("Could not parse next ts callback for %s #%d: %s", entry.class_name, entry.id, err.Error())
				err = nil
				continue
			}
		} else {
			entry.have_next_ts_callback = false
		}

		if err != nil {
			log.Errorf("Scan error in loadNewIds: %s", err.Error())
			err = nil
			continue
		}

		allSettingsMutex.Lock()
		allSettings[entry.id] = entry
		allSettingsMutex.Unlock()
	}

	return nil
}

func getGroupedScriptsForPlatform() (map[string]*ScriptEntry, error) {
	res := make(map[string]*ScriptEntry)

	rows, err := db.Query(QUERY_SIMPLE_GET_SCRIPTS_FOR_PLATFORM)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		entry := new(ScriptEntry)
		err = rows.Scan(
			&entry.class_name,
			&entry.settings_id)

		if err != nil {
			return nil, err
		}

		res[entry.class_name] = entry
	}

	return res, nil
}

// return map[location][class_name]*JobInfoEntry
func getGroupedJobInfo() (map[string]map[string]*JobInfoEntry, error) {
	res := make(map[string]map[string]*JobInfoEntry)

	rows, err := db.Query(QUERY_GET_JOB_INFO)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		entry := new(JobInfoEntry)
		err = rows.Scan(
			&entry.generation_id,
			&entry.class_name,
			&entry.location,
			&entry.init_jobs_ts,
			&entry.jobs_generated_ts,
			&entry.finish_jobs_ts,
			&entry.next_generate_job_ts,
			&entry.settings_id)

		if err != nil {
			return nil, err
		}

		if _, ok := res[entry.class_name]; !ok {
			res[entry.class_name] = make(map[string]*JobInfoEntry)
		}

		res[entry.class_name][entry.location] = entry
	}

	return res, nil
}

func getFlags() (map[string]*FlagEntry, error) {
	res := make(map[string]*FlagEntry)

	rows, err := db.Query(QUERY_GET_FLAGS)
	if err != nil {
		return nil, err
	}

	for rows.Next() {
		entry := new(FlagEntry)
		err = rows.Scan(
			&entry.class_name,
			&entry.run_requested_ts,
			&entry.run_accepted_ts,
			&entry.pause_requested_ts,
			&entry.kill_requested_ts,
			&entry.kill_request_employee_id,
			&entry.run_queue_killed_ts,
			&entry.killed_ts,
			&entry.paused_ts)

		if err != nil {
			return nil, err
		}

		res[entry.class_name] = entry
	}

	return res, nil
}

func getScriptRusageStats() (map[string]*ScriptRusageEntry, error) {
	res := make(map[string]*ScriptRusageEntry)

	rows, err := db.Query(QUERY_GET_SCRIPTS_RUSAGE_STATS)
	if err != nil {
		return nil, err
	}

	var (
		class_name string
		max_memory int64
		user_time  float64
		sys_time   float64
		real_time  float64
	)

	for rows.Next() {
		entry := new(ScriptRusageEntry)
		err = rows.Scan(
			&class_name,
			&real_time,
			&sys_time,
			&user_time,
			&max_memory)

		if err != nil {
			return nil, err
		}

		entry.cpu_usage = defaultRusage
		entry.max_memory = defaultMaxMemory

		if max_memory > 0 {
			entry.max_memory = max_memory
		}

		if (user_time > 0 || sys_time > 0) && real_time > 0 {
			entry.cpu_usage = (user_time + sys_time) / real_time
		}

		res[class_name] = entry
	}

	return res, nil
}

func getAvailableHosts() (map[string][]string, map[string]*ServerInfo, error) {
	rows, err := db.Query(QUERY_GET_AVAILABLE_HOSTS)
	if err != nil {
		return nil, nil, err
	}

	hosts := make(map[string][]string)
	info := make(map[string]*ServerInfo)

	defer rows.Close()

	for rows.Next() {
		entry := new(ServerInfo)

		err = rows.Scan(
			&entry.hostname,
			&entry.group,
			&entry.cpu_idle_parrots,
			&entry.cpu_parrots_per_core,
			&entry.cpu_idle_cores,
			&entry.cpu_cores,
			&entry.cpu_parasite,
			&entry.mem_total,
			&entry.mem_free,
			&entry.mem_cached,
			&entry.mem_parasite,
			&entry.swap_used,
			&entry.min_memory,
			&entry.min_memory_ratio)

		if err != nil {
			return nil, nil, err
		}

		hosts[entry.group] = append(hosts[entry.group], entry.hostname)
		info[entry.hostname] = entry
	}

	return hosts, info, nil
}

func clearOldHeartbeats() (err error) {
	_, err = db.Exec(QUERY_QUERY_CLEAR_OLD_HEARTBEATS, "class_name", `\ScriptFramework\Script_PhproxydHeartbeat`)
	return
}
