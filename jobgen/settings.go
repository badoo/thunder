package jobgen

import (
	"badoo/_packages/log"
	"database/sql"
	"errors"
	"time"
)

type (
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
		debug_enabled         int
		named_params          sql.NullString
		created               int64
	}
)

func loadSettingsFromRows(jiRows map[string]map[string]*JobInfoEntry, scripts map[string]*ScriptEntry) error {
	newIds := make(map[uint64]bool)
	jiRowsCnt := 0

	func() {
		allSettingsMutex.Lock()
		defer allSettingsMutex.Unlock()

		for _, row := range scripts {
			if _, ok := allSettings[row.settings_id]; !ok {
				newIds[row.settings_id] = true
			}
		}

		for _, rows := range jiRows {
			for _, row := range rows {
				jiRowsCnt++

				if _, ok := allSettings[row.settings_id]; !ok {
					newIds[row.settings_id] = true
				}
			}
		}
	}()

	if len(newIds) > 0 {
		loadNewIdsTs := time.Now().UnixNano()
		err := loadNewIds(newIds)
		if err != nil {
			return err
		}
		log.Printf("Loaded %d new ids for %.5f sec", len(newIds), float64(time.Now().UnixNano()-loadNewIdsTs)/1e9)
	}

	log.Debugln("  Selected", len(scripts), "rows from scripts")
	log.Debugln("  Selected", jiRowsCnt, "rows from job info")
	return nil
}

func getScriptSettings(className string) (*ScriptSettings, error) {
	scriptsMap.Lock()
	defer scriptsMap.Unlock()

	script := scriptsMap.v[className]
	if script == nil {
		return nil, errors.New("Unknown class: " + className)
	}

	settings := script.settings
	if settings == nil {
		return nil, errors.New("Damaged script entry for class " + className + ": there are no settings")
	}

	return settings, nil
}
