package logscol

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/badoo/thunder/common"
	"github.com/golang/protobuf/proto"
	"gopkg.in/fsnotify.v1"
)

const (
	OLD_FILE_SUFFIX              = ".old"
	MAX_LOG_FILE_SIZE_PRODUCTION = 20e6
	MAX_LINES                    = 1000
	MAX_ROTATE_TRIES             = 1000
	ROTATE_RETRY_SLEEP           = time.Second * 10
)

type (
	listener struct {
		filename string
		evChan   chan bool
	}

	rotateEvent struct {
		oldName string
		newName string
	}

	offsetsDbEntry struct {
		Ino uint64
		Off int64
	}
)

var (
	offsetsDb     string
	sourceDir     string
	targetAddress string
	conn          net.Conn

	maxLogFileSize int64

	fsNotifyEvChan  = make(chan string, 1000)
	rotateEvChan    = make(chan *rotateEvent, 10)
	outMessagesChan = make(chan *common.LogLines, 10)

	offsets      = make(map[uint64]int64) // inode => offset
	offsetsMutex sync.Mutex
)

// goroutine that starts to watch directory "sourceDir" and sends file names of changed files to "fsNotifyEvChan" chan
func inotifyEvSender() {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatalln("Could not set file system watcher: " + err.Error())
	}
	defer watcher.Close()

	if err = watcher.Add(sourceDir); err != nil {
		log.Fatalln("Could not start watching source directory: " + err.Error())
	}

	for {
		select {
		case ev := <-watcher.Events:
			if ev.Op&fsnotify.Write == fsnotify.Write {
				fsNotifyEvChan <- filepath.Base(ev.Name)
			}
		case err = <-watcher.Errors:
			log.Fatalln("Watch error occured: " + err.Error())
		}
	}
}

func streamLogs(filename string, chEvs chan bool, rotEvs chan *rotateEvent) {
	log.Println("Streaming ", filename)

	canRotate := !strings.HasSuffix(filename, OLD_FILE_SUFFIX)
	requestedRotate := false

	fp, err := os.Open(sourceDir + "/" + filename)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Println("Could not open ", filename, ":", err.Error())
		}
		log.Println("No such file: " + filename)
		return
	}
	defer fp.Close()

	fi, err := fp.Stat()
	if err != nil {
		log.Println("Could not stat ", filename, ": ", err.Error())
		return
	}

	ino := fi.Sys().(*syscall.Stat_t).Ino

	offsetsMutex.Lock()
	offset := offsets[ino]
	offsetsMutex.Unlock()

	if _, err = fp.Seek(offset, os.SEEK_SET); err != nil {
		log.Println("Could not seek to ", offset, ": ", err.Error())
		return
	}

	r := bufio.NewReaderSize(fp, 65536)

	log.Println("Listening events for " + filename)

	for {
		select {
		case <-chEvs:
			// log.Println("File changed: " + filename)

			if err := sendChanges(filename, r, ino, &offset); err != nil {
				log.Println("Stopping to stream ", filename, ", error ocurred: ", err.Error())
				return
			}

			if offset >= maxLogFileSize && canRotate && !requestedRotate {
				requestedRotate = true
				go requestRotate(filename)
			}
		case ev := <-rotEvs:
			if ev.oldName == filename {
				// Some events could be lost or delivered in wrong order during rotate, so force re-processing
				log.Println("Received event about rotated " + filename + ", updating filename to " + ev.newName)
				filename = ev.newName
				canRotate = false
				fsNotifyEvChan <- filename
				break
			} else {
				// we already were listening the rotated file; now it is deleted, so stop listening
				log.Println("Stopping to listen " + filename)
				return
			}
		}
	}
}

func sendChanges(filename string, r *bufio.Reader, ino uint64, offset *int64) error {
	newOffset := *offset

	var lines []string

	for {
		str, err := r.ReadString('\n')
		if err == io.EOF {
			if len(str) == 0 {
				break
			}
		} else if err != nil {
			log.Println("Could not read file: ", err.Error())
			return err
		}

		lines = append(lines, str)
		newOffset += int64(len(str))

		if len(lines) >= MAX_LINES {
			fsNotifyEvChan <- filename // trigger event again so we would read the rest
			break
		}
	}

	if len(lines) > 0 {
		outMessagesChan <- &common.LogLines{FileName: proto.String(strings.TrimSuffix(filename, OLD_FILE_SUFFIX)), Offset: proto.Int64(newOffset), Inode: proto.Uint64(ino), Lines: lines}
	}

	*offset = newOffset
	return nil
}

func requestRotate(filename string) {
	fp, err := os.OpenFile(sourceDir+"/"+filename+OLD_FILE_SUFFIX, os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			doRotate(filename, 0)
		} else {
			log.Println("Could not open ", filename, ":", err.Error())
		}
		return
	}
	defer fp.Close()

	if err := syscall.Flock(int(fp.Fd()), syscall.LOCK_EX); err != nil {
		log.Println("Could not obtain exclusive lock for file " + fp.Name() + ", it will not be rotated: " + err.Error())
		return
	}

	fi, err := fp.Stat()
	if err != nil {
		log.Println("Could not do fstat on file " + fp.Name() + ": " + err.Error())
		return
	}

	fileSz, err := fp.Seek(0, os.SEEK_END)
	if err != nil {
		log.Println("Could not seek " + fp.Name() + " until end: " + err.Error())
		return
	}

	ino := fi.Sys().(*syscall.Stat_t).Ino

	tries := 0

	for {
		offsetsMutex.Lock()
		readOff := offsets[ino]
		offsetsMutex.Unlock()

		if readOff >= fileSz {
			break
		}

		log.Println("File ", filename, " is not fully sync: readoff=", readOff, ", filesz=", fileSz)

		fsNotifyEvChan <- filename

		if tries >= MAX_ROTATE_TRIES {
			log.Println("Could not wait for " + fp.Name() + " to be transferred, not rotating")
			return
		}

		tries++
		time.Sleep(ROTATE_RETRY_SLEEP)
	}

	doRotate(filename, ino)
}

func doRotate(filename string, ino uint64) {
	log.Println("Rotating " + filename)

	if err := os.Rename(sourceDir+"/"+filename, sourceDir+"/"+filename+OLD_FILE_SUFFIX); err != nil {
		log.Println("Could not rotate file " + filename + ": " + err.Error())
		return
	}

	offsetsMutex.Lock()
	delete(offsets, ino)
	offsetsMutex.Unlock()

	rotateEvChan <- &rotateEvent{oldName: filename, newName: filename + OLD_FILE_SUFFIX}
}

func inotifyRouter() {
	chEvs := make(map[string]chan bool)
	evNames := make(map[chan bool]string)
	rotEvs := make(map[chan bool]chan *rotateEvent)
	unsub := make(chan chan bool, 1)

	for {
		select {
		case ev := <-rotateEvChan:
			// Upon rotate, we need to send event to two streamers:
			//   1. Current file streamer
			//   2. "old" file streamer - the file that deleted during rotate

			ch, ok := chEvs[ev.oldName]
			if !ok {
				break
			}

			evNames[ch] = ev.newName

			if oldCh, ok := chEvs[ev.newName]; ok {
				select {
				case rotEvs[oldCh] <- ev:
				default:
				}
			}

			chEvs[ev.newName] = ch
			delete(chEvs, ev.oldName)

			select {
			case rotEvs[ch] <- ev:
			default:
			}
		case name := <-fsNotifyEvChan:
			// log.Println("got event about " + name)

			ch, ok := chEvs[name]
			if !ok {
				ch = make(chan bool, 1)
				chEvs[name] = ch
				evNames[ch] = name

				// maximum 2 rotate events can happen - first is rename to .old, second is delete of .old file
				rotCh := make(chan *rotateEvent, 2)
				rotEvs[ch] = rotCh
				go func(name string, ch chan bool, rotCh chan *rotateEvent) {
					streamLogs(name, ch, rotCh)
					unsub <- ch
				}(name, ch, rotCh)
			}

			// log.Println("channel ", ch)
			// log.Println("sending event about " + name)
			select {
			case ch <- true:
			default:
			}

			// log.Println("sent event about " + name)
		case ch := <-unsub:
			if name, ok := evNames[ch]; ok {
				// log.Println("Unsubscribing " + name)
				delete(evNames, ch)
				delete(rotEvs, ch)
				if chEvs[name] == ch {
					delete(chEvs, name)
				}
			}
		}
	}
}

func saveOffsets() {
	fp, err := os.OpenFile(offsetsDb+".tmp", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatalln("Could not create tmp file for saving offsets: ", err.Error())
	}

	offsetsMutex.Lock()
	jsonDb := make([]*offsetsDbEntry, 0, len(offsets))

	for ino, off := range offsets {
		jsonDb = append(jsonDb, &offsetsDbEntry{Ino: ino, Off: off})
	}

	err = json.NewEncoder(fp).Encode(jsonDb)

	offsetsMutex.Unlock()

	if err != nil {
		log.Fatalln("Could not marshal offsets to JSON: ", err.Error())
	}

	if err = fp.Close(); err != nil {
		log.Fatalln("Could not close offsets file: ", err.Error())
	}

	if err = os.Rename(fp.Name(), offsetsDb); err != nil {
		log.Fatalln("Could not rename temp offsets db to permanent one")
	}
}

func offsetsSaver() {
	for {
		time.Sleep(time.Second)
		saveOffsets()
	}
}

func InitFlags() {
	flag.StringVar(&offsetsDb, "offsets-db", "/local/tmp/logscoll-offsets.db", "File where to store read logs offsets")
	flag.StringVar(&sourceDir, "source-dir", "/local/logs/scripts/phprocksyd/", "Script logs directory from which to collect logs")
	flag.StringVar(&targetAddress, "target-address", "cloudlogs:1065", "Cloud logs server address")
}

func getAllInodes(dir string) (map[uint64]bool, error) {
	result := make(map[uint64]bool)
	fp, err := os.Open(sourceDir)
	if err != nil {
		return nil, err
	}
	defer fp.Close()

	var fis []os.FileInfo
	for {
		fis, err = fp.Readdir(100)
		if err != nil {
			if err == io.EOF {
				break
			} else {
				return nil, err
			}
		}

		for _, fi := range fis {
			result[fi.Sys().(*syscall.Stat_t).Ino] = true
		}
	}

	return result, nil
}

func collectInitial() {
	fp, err := os.Open(sourceDir)
	if err != nil {
		log.Panicln("Could not open " + sourceDir + " for initial collect: " + err.Error())
	}
	defer fp.Close()

	var names []string
	for {
		names, err = fp.Readdirnames(100)
		if err != nil {
			break
		}

		for _, name := range names {
			fsNotifyEvChan <- name
		}
	}
}

func Run() {
	var err error

	maxLogFileSize = MAX_LOG_FILE_SIZE_PRODUCTION

	log.Println("Estabilishing connection to logs server")
	conn, err = net.Dial("tcp", targetAddress)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Could not connect to cloud logs server: "+err.Error())
		os.Exit(1)
	}

	log.Println("Reading offsets db")
	fp, err := os.Open(offsetsDb)
	if err == nil {

		offsets = make(map[uint64]int64)
		jsonDb := make([]*offsetsDbEntry, 0)

		err = json.NewDecoder(fp).Decode(&jsonDb)

		fp.Close()

		if err != nil {
			log.Println("Could not unmarshal offsets db: ", err.Error())
		} else {
			for _, el := range jsonDb {
				offsets[el.Ino] = el.Off
			}
		}

		allInodes, err := getAllInodes(sourceDir)

		if err != nil {
			log.Panicln("Could not get all inodes from ", sourceDir, ": ", err.Error())
		}

		for ino := range offsets {
			if _, ok := allInodes[ino]; !ok {
				delete(offsets, ino)
			}
		}

	}

	log.Println("Saving offsets db")
	saveOffsets()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-c
		log.Println("Got termination signal, saving offsets database")
		saveOffsets()
		os.Exit(1)
	}()

	log.Println("Entering main cycle")

	go inotifyEvSender()
	go inotifyRouter()
	go offsetsSaver()
	go collectInitial()
	go func() {
		for {
			err := common.SendMessage(conn, <-outMessagesChan)
			if err != nil {
				saveOffsets()
				log.Fatalln("Could not send outgoing message to logs server: ", err.Error())
			}
		}
	}()

	for {
		ln, err := common.ReceiveMessage(conn)
		if err != nil {
			log.Fatalln("Could not receive answer from processor: ", err.Error())
		}

		offsetsMutex.Lock()
		offsets[*ln.Inode] = *ln.Offset
		offsetsMutex.Unlock()
	}
}
