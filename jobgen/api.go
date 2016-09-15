package jobgen

import (
	"github.com/badoo/thunder/db"
	"github.com/badoo/thunder/proto"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const MAX_API_JOBS = 50000

func APIAcceptTTJobs(jobs []*thunder.RequestAddJobsJobT) ([]uint64, error) {
	now := uint64(time.Now().Unix())

	classLocType := make(map[string]string) // class_name => location_type
	ttRows := make([]*TimetableEntry, 0, len(jobs))

	perClassLoc := make(map[string]map[string][]*TimetableEntry)

	for _, row := range jobs {
		settings, err := getScriptSettings(row.GetClassName())
		if err != nil {
			return nil, err
		}

		classLocType[row.GetClassName()] = settings.location_type

		jrow := new(TimetableEntry)
		jrow.class_name = row.GetClassName()

		if row.Repeat == nil {
			jrow.repeat = settings.repeat_job
		} else {
			if row.Repeat.Value == nil {
				jrow.repeat.Valid = false
			} else {
				jrow.repeat.Valid = true
				jrow.repeat.Int64 = int64(row.Repeat.GetValue())
			}
		}

		jrow.default_retry = settings.retry_job

		jrow.created = now

		if row.SettingsId == nil {
			jrow.settings_id = settings.id
			jrow.settings = settings
		} else {
			jrow.settings_id = row.GetSettingsId()
			allSettingsMutex.Lock()
			jrow.settings = allSettings[jrow.settings_id]
			allSettingsMutex.Unlock()

			if jrow.settings == nil {
				ids := make(map[uint64]bool)
				ids[jrow.settings_id] = true
				err := loadNewIds(ids)
				if err != nil {
					return nil, err
				}

				allSettingsMutex.Lock()
				jrow.settings = allSettings[jrow.settings_id]
				allSettingsMutex.Unlock()

				if jrow.settings == nil {
					return nil, errors.New(fmt.Sprintf("Incorrect value of settings_id: %v", jrow.settings_id))
				}
			}

			if jrow.settings.location_type != settings.location_type {
				return nil, errors.New(fmt.Sprintf("You are not allowed to specify settings_id that has different location_type, row: %+v", row))
			}
		}

		jrow.NextLaunchTs.Valid = true

		if row.NextLaunchTs == nil {
			jrow.NextLaunchTs.Int64 = int64(now)
		} else {
			jrow.NextLaunchTs.Int64 = row.GetNextLaunchTs()
		}

		if row.Location == nil {
			jrow.location = settings.location
		} else {
			jrow.location = row.GetLocation()

			if settings.location_type == LOCATION_TYPE_ANY && jrow.settings.location != settings.location {
				return nil, errors.New(fmt.Sprintf("For location_type=any scripts location field must be equal to current settings: %+v", row))
			}
		}

		jrow.JobData = row.GetJobData()

		if row.Method == nil {
			jrow.method = METHOD_RUN
		} else {
			jrow.method = row.GetMethod()
		}

		if row.GenerationId == nil {
			jrow.generation_id.Valid = false
		} else {
			jrow.generation_id.Valid = true
			jrow.generation_id.Int64 = row.GetGenerationId()
		}

		ttRows = append(ttRows, jrow)

		el, ok := perClassLoc[jrow.class_name]
		if !ok {
			el = make(map[string][]*TimetableEntry)
			perClassLoc[jrow.class_name] = el
		}

		el[jrow.location] = append(el[jrow.location], jrow)
	}

	for className, locRows := range perClassLoc {
		for location, rows := range locRows {
			key := DEFAULT_LOCATION_IDX
			if classLocType[className] == LOCATION_TYPE_EACH {
				key = location
			}

			currentCnt := 0
			if ch := getDispatcherJobsCountCh(className, key); ch != nil {
				respCh := make(chan int, 1)
				ch <- &JobsCountRequest{RespCh: respCh}
				currentCnt = <-respCh
			}

			if currentCnt+len(rows) > MAX_API_JOBS {
				return nil, fmt.Errorf("Too many jobs: %d (current) + %d (adding) > %d (max)", currentCnt, len(rows), MAX_API_JOBS)
			}
		}
	}

	err := db.DoInLazyTransaction(func(tx *db.LazyTrx) error {
		return addToTimetable(tx, ttRows)
	})

	if err != nil {
		return nil, err
	}

	for className, locRows := range perClassLoc {
		for location, rows := range locRows {
			key := DEFAULT_LOCATION_IDX
			if classLocType[className] == LOCATION_TYPE_EACH {
				key = location
			}

			notifyAboutNewTTRows(className, key, rows, false)
		}
	}

	ids := make([]uint64, 0, len(ttRows))
	for _, row := range ttRows {
		ids = append(ids, row.id)
	}

	return ids, nil
}

func APIUpdateRunStatus(hostname string, runId uint64, prevStatus, status string) error {
	ch, err := getLauncherUpdateStatusCh(hostname)
	if err != nil {
		return err
	}

	errCh := make(chan error, 1)

	ch <- &LauncherUpdateStatusRequest{
		Hostname:   hostname,
		RunId:      runId,
		PrevStatus: prevStatus,
		Status:     status,
		errCh:      errCh,
	}

	return <-errCh
}

// string is json-encode'd run queue entry row
func APILogFinish(hostname string, runId uint64, prevStatus string, success bool) (string, error) {
	ch, err := getLauncherLogFinishCh(hostname)
	if err != nil {
		return "", err
	}

	respCh := make(chan *LauncherLogFinishResponse, 1)

	ch <- &LauncherLogFinishRequest{
		Hostname:   hostname,
		RunId:      runId,
		PrevStatus: prevStatus,
		Success:    success,
		respCh:     respCh,
	}

	res := <-respCh
	if res.err != nil {
		return "", res.err
	}

	data, err := json.Marshal(res.info)
	if err != nil {
		return "", err
	}

	return string(data), nil
}
