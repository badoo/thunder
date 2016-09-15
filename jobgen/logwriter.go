package jobgen

import (
	"bufio"
	"os"
	"time"

	"badoo/_packages/log"
	"encoding/json"
)

type (
	FinishResultRusage struct {
		UserTime  float64 `json:"user_time"`
		SysTime   float64 `json:"sys_time"`
		RealTime  float64 `json:"real_time"`
		MaxMemory uint64  `json:"max_memory"`
	}

	FinishResultRunInfo struct {
		Id                uint64      `json:"id"`
		TimetableId       uint64      `json:"timetable_id"`
		GenerationId      uint64      `json:"generation_id"`
		Hostname          string      `json:"hostname"`
		HostnameIdx       uint32      `json:"hostname_idx"`
		ClassName         string      `json:"class_name"`
		JobData           string      `json:"job_data"`
		Method            string      `json:"method"`
		RunStatus         string      `json:"run_status"`
		Created           interface{} `json:"created"`
		WaitingTs         interface{} `json:"waiting_ts"`
		ShouldInitTs      interface{} `json:"should_init_ts"`
		InitAttempts      uint32      `json:"init_attempts"`
		InitTs            interface{} `json:"init_ts"`
		RunningTs         interface{} `json:"running_ts"`
		MaxFinishedTs     interface{} `json:"max_finished_ts"`
		FinishedTs        interface{} `json:"finished_ts"`
		StoppedEmployeeId int64       `json:"stopped_employee_id"`
		Token             string      `json:"token"`
		RetryAttempt      uint32      `json:"retry_attempt"`
		SettingsId        uint64      `json:"settings_id"`
	}

	FinishResult struct {
		Id           uint64              `json:"id"`
		TimetableId  uint64              `json:"timetable_id"`
		ClassName    string              `json:"class_name"`
		Hostname     string              `json:"hostname"`
		Success      bool                `json:"success"`
		PrevStatus   string              `json:"prev_status"`
		Rusage       FinishResultRusage  `json:"rusage"`
		ProfilingUrl string              `json:"profiling_url"`
		Initial      bool                `json:"initial"`
		Timestamp    uint64              `json:"timestamp"`
		RunInfo      FinishResultRunInfo `json:"run_info"`
	}
)

var (
	rqhLog = make(chan *FinishResult, 100)
)

func WriteLogsThread(filename string) {
	log.Infof("Started write logs thread to file=%s", filename)

	reopenTick := time.Tick(time.Second * 10)

	fp, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	var wr *bufio.Writer

	if err != nil {
		log.Errorf("Could not open %s: %s", filename, err.Error())
	} else {
		wr = bufio.NewWriterSize(fp, 65536)
	}

	for {
		select {
		case <-reopenTick:
			if fp != nil {
				fp.Close()
			}

			fp, err = os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				log.Warnf("Could not reopen %s: %s", err.Error())
				wr = nil
				fp = nil
			} else {
				wr = bufio.NewWriterSize(fp, 65536)
			}
		case ev := <-rqhLog:
			l := len(rqhLog)
			evs := make([]*FinishResult, 0, l+1)
			evs = append(evs, ev)

			for i := 0; i < l; i++ {
				evs = append(evs, <-rqhLog)
			}

			if wr != nil {
				encoder := json.NewEncoder(wr)

				for _, e := range evs {
					if err = encoder.Encode(e); err != nil {
						log.Errorf("Could not write to %s: %s", filename, err.Error())
					}
				}

				if err = wr.Flush(); err != nil {
					log.Errorf("Could not flush contents to %s: %s", filename, err.Error())
				}
			} else {
				log.Errorf("Failed to write %d events to rqh log because file %s could not be opened", len(evs), filename)
			}
		}

	}
}
