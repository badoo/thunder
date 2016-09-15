package jobgen

import (
	"encoding/json"
	"errors"
	"math/rand"
	"time"

	"badoo/_packages/log"
)

var defaultParasiteMemory int64

func getParasiteMemory(info *ServerInfo) int64 {
	if info.mem_parasite.Valid {
		return info.mem_parasite.Int64
	}

	return defaultParasiteMemory
}

func getFreeMem(info *ServerInfo) int64 {
	return info.mem_free.Int64 + info.mem_cached.Int64 - info.swap_used.Int64
}

func getNextJobGenerateTs(className string, firstRun bool, prevTs uint64, settings *ScriptSettings) (nextTs uint64) {
	if !settings.have_next_ts_callback {
		if firstRun {
			nextTs = uint64(time.Now().Unix())
		} else {
			nextTs = prevTs + uint64(settings.repeat)
		}
	} else if settings.next_ts_callback.Callback == "cron" {
		if _, ok := settings.next_ts_callback.Settings["cron"]; !ok {
			log.Warning("'cron' section is absent for next ts callback for " + className)
			return
		}

		now := uint64(time.Now().Unix())

		if firstRun {
			prevTs = now
		}

		nextTs = getNextLaunchTsForCron(prevTs, now, rand.Intn(60), settings.next_ts_callback.Settings["cron"])
	} else {
		log.Warning("Callbacks other than cron are not supported for " + className)
	}

	return
}

func getLocationIdx(locationType string, location string) (res string, err error) {
	if locationType == LOCATION_TYPE_EACH {
		res = location
	} else if locationType == LOCATION_TYPE_ANY {
		res = DEFAULT_LOCATION_IDX
	} else {
		err = errors.New("Incorrect location type: " + locationType)
	}

	return
}

func parseJobs(jobsStr string) (jobs Jobs, err error) {
	err = json.Unmarshal([]byte(jobsStr), &jobs)
	return
}

func parseNextTsCallback(nextTsCallbackStr string) (callback NextTsCallback, err error) {
	err = json.Unmarshal([]byte(nextTsCallbackStr), &callback)
	return
}
