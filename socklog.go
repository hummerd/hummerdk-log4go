// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"sync"
)

// This log writer sends output to a socket
type SocketLogWriter struct {
	rec       chan *LogRecord
	closeSync *sync.WaitGroup
}

// This is the SocketLogWriter's output method
func (w SocketLogWriter) LogWrite(rec *LogRecord) {
	w.rec <- rec
}

func (w SocketLogWriter) SelCloseSync(closeSync *sync.WaitGroup) {
	w.closeSync = closeSync
}

func (w SocketLogWriter) Close() {
	close(w.rec)
}

func NewSocketLogWriter(proto, hostport string) *SocketLogWriter {
	sock, err := net.Dial(proto, hostport)
	if err != nil {
		fmt.Fprintf(os.Stderr, "NewSocketLogWriter(%q): %s\n", hostport, err)
		return nil
	}

	w := &SocketLogWriter{
		rec: make(chan *LogRecord, LogBufferLength),
	}

	go func() {
		defer func() {
			if sock != nil && proto == "tcp" {
				sock.Close()
			}
			if w.closeSync != nil {
				w.closeSync.Done()
			}
		}()

		for rec := range w.rec {
			// Marshall into JSON
			js, err := json.Marshal(rec)
			if err != nil {
				fmt.Fprint(os.Stderr, "SocketLogWriter(%q): %s", hostport, err)
				return
			}

			_, err = sock.Write(js)
			if err != nil {
				fmt.Fprint(os.Stderr, "SocketLogWriter(%q): %s", hostport, err)
				return
			}
		}
	}()

	return w
}
