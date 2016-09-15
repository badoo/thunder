package jobgen

import (
	"badoo/_packages/log"
	"sync"
)

var (
	killerThreads struct {
		v map[string]map[string]bool // class_name => map(dispatcher_location => true)
		sync.Mutex
	}
)

func startKilling(className string) {
	killerThreads.Lock()
	defer killerThreads.Unlock()

	dispatchThreads.Lock()
	defer dispatchThreads.Unlock()

	locs, ok := dispatchThreads.v[className]
	if !ok {
		return
	}

	killMap := killerThreads.v[className]
	if killMap == nil {
		killMap = make(map[string]bool)
		killerThreads.v[className] = killMap
	}

	for loc, dt := range locs {
		if killMap[loc] {
			continue
		}

		req := &KillRequest{
			ResCh: make(chan error, 1),
		}

		log.Printf("Sending kill request to class=%s, location=%s", className, loc)
		select {
		case dt.killRequestCh <- req:
			killMap[loc] = true
		default:
			log.Warnf("Could not send kill request to class=%s, location=%s, kill channel was busy", className, loc)
		}
	}
}

func continueDispatchAfterKill(className string) {
	killerThreads.Lock()
	defer killerThreads.Unlock()

	delete(killerThreads.v, className)
}
