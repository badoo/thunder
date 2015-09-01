package logsproc

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/badoo/thunder/common"
)

type (
	msg struct {
		lines []string
		resp  chan error
	}
)

var (
	targetDir     string
	listenAddress string

	fileChans      = make(map[string]chan *msg) // nil message means that file reopen is required
	fileChansMutex sync.Mutex
)

func InitFlags() {
	flag.StringVar(&targetDir, "target-dir", "/local/logs/sf", "Target directory where to put logs")
	flag.StringVar(&listenAddress, "listen-address", "0.0.0.0:1065", "Listen address")
}

func Run() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGUSR1)
	go func() {
		for {
			<-c
			log.Println("Forcing re-open for log files")

			fileChansMutex.Lock()
			for _, ch := range fileChans {
				ch <- nil
			}
			fileChansMutex.Unlock()
		}
	}()

	ln, err := net.Listen("tcp", listenAddress)
	if err != nil {
		log.Fatalln("Could not listen supplied port: " + err.Error())
	}

	log.Println("Listening on", listenAddress)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Fatalln("Could not accept connection: " + err.Error())
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	for {
		ln, err := common.ReceiveMessage(conn)
		if err != nil {
			if err != io.EOF {
				log.Println("Got error whe receiving message: ", err.Error())
			}

			return
		}

		writeLines(ln)

		// write response about file and offset
		if err = common.SendMessage(conn, &common.LogLines{FileName: ln.FileName, Inode: ln.Inode, Offset: ln.Offset}); err != nil {
			log.Println("Could not send message:", err.Error())
			return
		}
	}
}

func openFile(filename string) *os.File {
	targetPath := targetDir + "/" + filepath.Clean(filename)

	fp, err := os.OpenFile(targetPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatalln("Could not open " + filename + " for writing: " + err.Error())
	}

	return fp
}

func writeThread(filename string, ch chan *msg) {
	var err error
	fp := openFile(filename)
	w := bufio.NewWriterSize(fp, 65536)

	for {
		messages := make([]*msg, 0)
		messages = append(messages, <-ch)
		if l := len(ch); l > 0 {
			for l > 0 {
				messages = append(messages, <-ch)
				l--
			}
		}

		for _, m := range messages {
			if m == nil {
				if err = w.Flush(); err != nil {
					log.Fatalln("Could not flush write: " + err.Error())
				}
				fp.Close()
				fp = openFile(filename)
				w = bufio.NewWriter(fp)
				continue
			}

			for _, s := range m.lines {
				if _, err = w.WriteString(s); err != nil {
					log.Fatalln("Could not write logs: " + err.Error())
				}
			}
		}

		if err = w.Flush(); err != nil {
			log.Fatalln("Could not flush writes: " + err.Error())
		}

		for _, m := range messages {
			if m != nil {
				m.resp <- nil
			}
		}
	}
}

func writeLines(ln *common.LogLines) (err error) {
	fileChansMutex.Lock()
	ch, ok := fileChans[*ln.FileName]
	if !ok {
		ch = make(chan *msg, 500)
		go writeThread(*ln.FileName, ch)
		fileChans[*ln.FileName] = ch
	}
	fileChansMutex.Unlock()

	respCh := make(chan error, 1)
	ch <- &msg{lines: ln.Lines, resp: respCh}
	err = <-respCh

	return
}
