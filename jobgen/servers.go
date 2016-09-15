package jobgen

import (
	"badoo/_packages/log"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"
)

type (
	ScriptRusageEntry struct {
		cpu_usage  float64
		max_memory int64
		host       string
	}

	ServerInfo struct {
		hostname             string
		group                string
		cpu_user             sql.NullFloat64
		cpu_sys              sql.NullFloat64
		cpu_nice             sql.NullFloat64
		cpu_iowait           sql.NullFloat64
		cpu_steal            sql.NullFloat64
		cpu_idle             sql.NullFloat64
		cpu_parasite         sql.NullFloat64
		cpu_parrots_per_core uint
		cpu_cores            uint
		mem_total            sql.NullInt64
		mem_free             sql.NullInt64
		mem_cached           sql.NullInt64
		mem_buffers          sql.NullInt64
		mem_parasite         sql.NullInt64
		swap_total           sql.NullInt64
		swap_used            sql.NullInt64
		min_memory           sql.NullInt64
		min_memory_ratio     sql.NullFloat64

		cpu_idle_cores      float64
		cpu_idle_parrots    float64
		real_cpu_idle_cores float64

		real_mem_free   uint64
		real_mem_cached uint64
		real_swap_used  uint64
	}
)

var (

	// default values from config
	defaultMinIdleCpu     float64
	defaultMinMemory      int64
	defaultMinMemoryRatio float64
	defaultMaxMemory      int64
	defaultRusage         float64

	// rusage statistics
	rusageInfo struct {
		groupsMaxParrots map[string]uint64
		groupHosts       map[string][]string
		hostsInfo        map[string]*ServerInfo
		loadEstimate     map[string]*ScriptRusageEntry
		timetableRusage  map[uint64]*ScriptRusageEntry
		groupIdx         map[string]uint64 // index for current group used for weighted round-robin algorithm

		sync.Mutex
	}

	failedLocations      = make(map[string]bool)
	failedLocationsMutex sync.Mutex
)

func updateHosts() {
	hosts, info, err := getAvailableHosts()
	if err != nil {
		log.Errorf("Could not get available hosts: %s", err.Error())
		return
	}

	rusageInfo.Lock()
	rusageInfo.groupHosts = hosts
	rusageInfo.hostsInfo = info
	rusageInfo.Unlock()

	// TODO: update max parrtos as well
}

func updateHostsThread() {
	for {
		time.Sleep(HOSTS_UPDATE_INTERVAL)
		updateHosts()
	}
}

func getLocations(settings *ScriptSettings) []string {
	rusageInfo.Lock()
	defer rusageInfo.Unlock()

	result := make([]string, 0)

	if settings.location_type == LOCATION_TYPE_EACH {
		location := make([]string, 0)

		if settings.location == LOCATION_ALL {
			for group := range rusageInfo.groupHosts {
				location = append(location, group)
			}
		} else {
			location = strings.Split(settings.location, ",")
		}

		for _, loc := range location {
			if _, ok := rusageInfo.groupHosts[loc]; ok {
				for _, hostname := range rusageInfo.groupHosts[loc] {
					result = append(result, hostname)
				}
			} else {
				logFailedLocation(settings, loc, "no such group")
			}
		}
	} else {
		result = append(result, settings.location)
	}

	return result
}

func logFailedLocation(settings *ScriptSettings, location, failureReason string) {
	failedLocationsMutex.Lock()
	defer failedLocationsMutex.Unlock()

	if failedLocations[location] {
		return
	}

	failedLocations[location] = true

	log.Println("Failed location " + location + " for script " + settings.class_name + ": " + failureReason)
}

var (
	errorNoLocationInHostsInfo = errors.New("No location in hosts info")
	errorThisShouldNeverHappen = errors.New("This should never happen (no hosts info, but present in group hosts)")
	errorReduceLoadOnWeakHosts = errors.New("Reduce load on weak hosts")
)

func selectHostname(location, locationType string, cpuUsage float64, maxMemory int64) (result string, err error) {
	rusageInfo.Lock()
	defer rusageInfo.Unlock()

	if locationType == LOCATION_TYPE_EACH {
		if _, ok := rusageInfo.hostsInfo[location]; ok {
			result = location
			return
		} else {
			err = errorNoLocationInHostsInfo
			return
		}
	}

	if _, ok := rusageInfo.groupHosts[location]; !ok {
		err = errors.New("No hosts in " + location)
		return
	}

	// put only 'alive' hosts into sourceHosts and count maxParrots among alive hosts
	sourceHosts := rusageInfo.groupHosts[location]
	hosts := make([]string, 0, len(sourceHosts))
	var maxParrots float64

	for _, hostname := range sourceHosts {
		info, ok := rusageInfo.hostsInfo[hostname]
		if !ok {
			err = errorThisShouldNeverHappen
			result = ""
			continue
		}

		if (info.cpu_idle_cores-cpuUsage)/float64(info.cpu_cores) < defaultMinIdleCpu {
			err = errors.New(fmt.Sprintf("Script would exceed MIN_IDLE_CPU (%f - %f) / %d < %f", info.cpu_idle_cores, cpuUsage, info.cpu_cores, defaultMinIdleCpu))
			result = ""
			continue
		}

		si := *info
		si.mem_free.Int64 -= maxMemory

		if exceeded, descr := memoryThresholdExceeded(&si); exceeded {
			err = errors.New(fmt.Sprint("Script would exceed memory threshold: ", descr))
			result = ""
			continue
		}

		hosts = append(hosts, hostname)
		if info.cpu_idle_parrots > maxParrots {
			maxParrots = info.cpu_idle_parrots
		}
	}

	maxParrotsInt := int64(maxParrots)
	if maxParrotsInt <= 1 {
		err = errors.New(fmt.Sprintf("No parrots in group. Last failure reason: %v", err))
		result = ""
		return
	}

	hostsCount := len(hosts)

	for i := 0; i < hostsCount; i++ {
		rusageInfo.groupIdx[location] = rusageInfo.groupIdx[location] % uint64(hostsCount)
		result = hosts[rusageInfo.groupIdx[location]]
		rusageInfo.groupIdx[location]++
		info := rusageInfo.hostsInfo[result]

		if float64(rand.Int63n(maxParrotsInt)) > info.cpu_idle_parrots {
			err = errorReduceLoadOnWeakHosts
			result = ""
			continue
		}

		if getWaitingRQLen(result) > SELECT_HOSTNAME_MAX_WAITING_LEN {
			err = fmt.Errorf("Too many waiting rows in %s", result)
			result = ""
			continue
		}

		info.mem_free.Int64 -= maxMemory
		info.cpu_idle_cores -= cpuUsage
		break
	}

	if len(result) > 0 {
		err = nil
	}

	return
}

func memoryThresholdExceeded(info *ServerInfo) (exceeded bool, description string) {
	memFree := getFreeMem(info)
	minMemory := defaultMinMemory

	if info.min_memory.Valid {
		minMemory = info.min_memory.Int64
	}

	if memFree < minMemory {
		description = fmt.Sprintf("low memory: %.2f MiB free", float64(memFree)/(1024*1024))
		exceeded = true
		return
	}

	if info.mem_total.Valid && info.mem_total.Int64 > 0 {
		currentRatio := float64(memFree) / float64(info.mem_total.Int64)

		minRatio := defaultMinMemoryRatio

		if info.min_memory_ratio.Valid {
			minRatio = info.min_memory_ratio.Float64
		}

		if currentRatio < minRatio {
			description = fmt.Sprintf("low memory: %.1f%% free", currentRatio)
			exceeded = true
			return
		}
	}

	return
}

func setLoadEstimates(rqRows map[string]map[string]map[string]*RunQueueEntry, scriptsRusage map[string]*ScriptRusageEntry) {
	rusageInfo.Lock()
	defer rusageInfo.Unlock()

	rusageInfo.timetableRusage = make(map[uint64]*ScriptRusageEntry)
	rusageInfo.loadEstimate = make(map[string]*ScriptRusageEntry)

	for className, locRows := range rqRows {
		for _, jobRows := range locRows {
			for _, row := range jobRows {
				if _, ok := scriptsRusage[className]; !ok {
					continue
				}

				rusageRow := scriptsRusage[className]
				host := row.hostname

				if _, ok := rusageInfo.loadEstimate[host]; !ok {
					rusageInfo.loadEstimate[host] = &ScriptRusageEntry{
						cpu_usage:  rusageRow.cpu_usage,
						max_memory: rusageRow.max_memory,
					}
				} else {
					rusageInfo.loadEstimate[host].cpu_usage += rusageRow.cpu_usage
					rusageInfo.loadEstimate[host].max_memory += rusageRow.max_memory
				}

				rusageRowCopy := new(ScriptRusageEntry)
				*rusageRowCopy = *rusageRow
				rusageRowCopy.host = host

				rusageInfo.timetableRusage[uint64(row.timetable_id.Int64)] = rusageRowCopy
			}
		}
	}

	// If our estimation is greater than current server load, take our estimation
	// This should prevent new script framework from overloading the 'fast' servers with jobs
	for host, info := range rusageInfo.loadEstimate {
		if _, ok := rusageInfo.hostsInfo[host]; !ok {
			continue
		}

		hi := rusageInfo.hostsInfo[host]

		hi.real_cpu_idle_cores = hi.cpu_idle_cores
		hi.real_mem_free = uint64(hi.mem_free.Int64)
		hi.real_mem_cached = uint64(hi.mem_cached.Int64)
		hi.real_swap_used = uint64(hi.swap_used.Int64)

		currentInfo := rusageInfo.hostsInfo[host]
		// cpu_parasite is relative to number of cores
		idleCores := float64(currentInfo.cpu_cores)*(1.0-currentInfo.cpu_parasite.Float64) - float64(info.cpu_usage)

		parasiteMemory := getParasiteMemory(currentInfo)

		usedMemory := parasiteMemory + int64(info.max_memory)
		if usedMemory == 0 {
			log.Warningf("Used memory for %s is 0", host)
			continue
		}

		// max_memory can use swap, but parasite memory is resident
		swapRatio := float64(parasiteMemory) / float64(usedMemory)

		memFree := currentInfo.mem_total.Int64 - int64(usedMemory) - int64(float64(currentInfo.swap_used.Int64)*swapRatio)
		currentFreeMem := getFreeMem(currentInfo)

		if idleCores < currentInfo.cpu_idle_cores {
			if idleCores > 0 {
				hi.cpu_idle_cores = idleCores
			} else {
				hi.cpu_idle_cores = 0
			}
		}

		if memFree < currentFreeMem {
			// empty cached and swap, because we use estimated values for memory
			hi.swap_used.Int64 = 0
			hi.mem_cached.Int64 = 0
			if memFree > 0 {
				hi.mem_free.Int64 = memFree
			} else {
				hi.mem_free.Int64 = 0
			}
		}
	}
}

// assumes that rusageInfo.Lock() is held
func updateLoadEstimates() {
	rusageInfo.Lock()
	defer rusageInfo.Unlock()

	rusageInfo.groupsMaxParrots = make(map[string]uint64)

	// Compute max performance of each server group
	for group, hostnames := range rusageInfo.groupHosts {
		for _, host := range hostnames {
			if _, ok := rusageInfo.hostsInfo[host]; !ok {
				continue
			}

			idle_parrots := rusageInfo.hostsInfo[host].cpu_idle_parrots
			if idle_parrots > float64(rusageInfo.groupsMaxParrots[group]) {
				rusageInfo.groupsMaxParrots[group] = uint64(idle_parrots)
			}
		}
	}

	rusageInfo.groupIdx = make(map[string]uint64)
	for key := range rusageInfo.groupHosts {
		rusageInfo.groupIdx[key] = 0
	}
}
