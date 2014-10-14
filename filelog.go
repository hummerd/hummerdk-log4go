// Copyright (C) 2010, Kyle Lemons <kyle@kylelemons.net>.  All rights reserved.

package log4go

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"
)

var fileNameRegexp = regexp.MustCompile(`^.*\.(\d{1,6})$`)

// This log writer sends output to a file
type FileLogWriter struct {
	rec chan *LogRecord
	rot chan bool

	// The opened file
	filename string
	file     *os.File

	// The logging format
	format string

	// File header/trailer
	header, trailer string

	// Rotate at linecount
	maxlines          int
	maxlines_curlines int

	// Rotate at size
	maxsize         int
	maxsize_cursize int

	// Rotate daily
	daily          bool
	daily_opendate int

	maxfiles int

	// Keep old logfiles (.001, .002, etc)
	rotate bool

	closeSync *sync.WaitGroup
}

// This is the FileLogWriter's output method
func (w *FileLogWriter) LogWrite(rec *LogRecord) {
	w.rec <- rec
}

func (w *FileLogWriter) SelCloseSync(closeSync *sync.WaitGroup) {
	w.closeSync = closeSync
}

func (w *FileLogWriter) Close() {
	close(w.rec)
}

// NewFileLogWriter creates a new LogWriter which writes to the given file and
// has rotation enabled if rotate is true.
//
// If rotate is true, any time a new log file is opened, the old one is renamed
// with a .### extension to preserve it.  The various Set* methods can be used
// to configure log rotation based on lines, size, and daily.
//
// The standard log-line format is:
//   [%D %T] [%L] (%S) %M
func NewFileLogWriter(fname string, rotate bool) *FileLogWriter {
	w := &FileLogWriter{
		rec:      make(chan *LogRecord, LogBufferLength),
		rot:      make(chan bool),
		filename: fname,
		format:   "[%D %T] [%L] (%S) %M",
		rotate:   rotate,
		maxfiles: 100,
	}

	go func() {
		defer func() {
			if w.file != nil {
				fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
				w.file.Close()
			}

			if w.closeSync != nil {
				w.closeSync.Done()
			}
		}()

		// open the file for the first time
		if err := w.intRotate(false); err != nil {
			fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
			return
		}

		for {
			select {
			case <-w.rot:
				if err := w.intRotate(true); err != nil {
					fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
					return
				}
			case rec, ok := <-w.rec:
				if !ok {
					return
				}
				now := time.Now()
				if (w.maxlines > 0 && w.maxlines_curlines >= w.maxlines) ||
					(w.maxsize > 0 && w.maxsize_cursize >= w.maxsize) ||
					(w.daily && now.Day() != w.daily_opendate) {
					if err := w.intRotate(true); err != nil {
						fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
						return
					}
				}

				// Perform the write
				n, err := fmt.Fprint(w.file, FormatLogRecord(w.format, rec))
				if err != nil {
					fmt.Fprintf(os.Stderr, "FileLogWriter(%q): %s\n", w.filename, err)
				}

				// Update the counts
				w.maxlines_curlines++
				w.maxsize_cursize += n
			}
		}
	}()

	return w
}

// Request that the logs rotate
func (w *FileLogWriter) Rotate() {
	w.rot <- true
}

// If this is called in a threaded context, it MUST be synchronized
func (w *FileLogWriter) intRotate(force bool) error {
	// Close any log file that may be open
	if w.file != nil {
		fmt.Fprint(w.file, FormatLogRecord(w.trailer, &LogRecord{Created: time.Now()}))
		w.file.Close()
	}

	var cur_lines int = 0
	var cur_size int = 0

	// If we are keeping log files, move it to the next available number
	if w.rotate {
		fi, err := os.Lstat(w.filename)
		if err == nil { // file exists
			needRotate := force

			if !needRotate {
				cur_size = int(fi.Size())
				cur_lines, err := lineCount(w.filename)
				if err != nil {
					return fmt.Errorf("Rotate: %s\n", err)
				}

				if w.maxsize > 0 && !needRotate {
					needRotate = cur_size >= w.maxsize
				}

				if w.maxlines > 0 && !needRotate {
					needRotate = cur_lines >= w.maxlines
				}
			}

			if needRotate {
				cur_size = 0
				cur_lines = 0

				// Shift names of existing log files
				err = renameOldFiles(w.filename, w.maxfiles)
				if err != nil {
					return fmt.Errorf("Rotate: %s\n", err)
				}
			}
		}
	}

	// Open the log file
	fd, err := os.OpenFile(w.filename, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	w.file = fd

	now := time.Now()
	fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: now}))

	// Set the daily open date to the current date
	w.daily_opendate = now.Day()

	// initialize rotation values
	w.maxlines_curlines = cur_lines
	w.maxsize_cursize = cur_size

	return nil
}

func renameOldFiles(fileName string, maxFiles int) error {
	dir := path.Dir(fileName)

	f, err := os.Open(dir)
	if err != nil {
		return err
	}

	if maxFiles == 1 {
		err = os.Remove(fileName)
		if err != nil {
			return fmt.Errorf("Rotate error: %s\n", err)
		}
		return nil
	}

	names, err := f.Readdirnames(-1)
	if err != nil {
		return fmt.Errorf("Rotate error: %s\n", err)
	}

	fileNums, nums := getFileNums(names, maxFiles)

	sort.Ints(nums)
	free := 1
	for n := range nums {
		if n > free {
			break
		}
		free++
	}

	if free >= maxFiles && maxFiles > 0 {
		lastNum := nums[len(nums)-1]
		lastFile := fileNums[lastNum]
		delete(fileNums, lastNum)
		err = os.Remove(path.Join(dir, lastFile))
		if err != nil {
			return fmt.Errorf("Rotate error: %s\n", err)
		}
	}

	if free > 1 {
		err = shiftFiles(free, nums, fileNums, dir, fileName)
		if err != nil {
			return fmt.Errorf("Rotate error: %s\n", err)
		}
	}

	// rename current file
	newName := fileName + ".0001"
	err = os.Rename(fileName, newName)
	if err != nil {
		return fmt.Errorf("Rotate: %s\n", err)
	}

	return nil
}

func getFileNums(fileNames []string, maxFiles int) (map[int]string, []int) {
	fileNums := make(map[int]string)

	for _, name := range fileNames {
		fileNum := fileNameRegexp.FindStringSubmatch(name)
		if fileNum != nil {
			num, _ := strconv.Atoi(fileNum[1])
			//skip files with index greater then maxFiles (maybe someone will use them?)
			if maxFiles <= 0 || num < maxFiles {
				fileNums[num] = name
			}
		}
	}

	nums := make([]int, len(fileNums))

	i := 0
	for key := range fileNums {
		nums[i] = key
		i++
	}

	return fileNums, nums
}

func shiftFiles(freeSlot int, nums []int, files map[int]string, dir string, fileName string) error {
	for i := len(nums) - 1; i >= 0; i-- {
		n := nums[i]
		if n >= freeSlot {
			continue
		}

		oldName, ok := files[n]
		if !ok {
			continue
		}

		oldFile := path.Join(dir, oldName)
		newFile := fileName + fmt.Sprintf(".%04d", n+1)
		err := os.Rename(oldFile, newFile)
		if err != nil {
			return fmt.Errorf("Rotate error: %s\n", err)
		}
	}

	return nil
}

func lineCount(fileName string) (int, error) {
	r, err := os.Open(fileName)
	defer r.Close()
	if err != nil {
		return 0, err
	}

	buf := make([]byte, 8196)
	count := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return count, err
		}

		count += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return count, nil
}

// Set the logging format (chainable).  Must be called before the first log
// message is written.
func (w *FileLogWriter) SetFormat(format string) *FileLogWriter {
	w.format = format
	return w
}

// Set the logfile header and footer (chainable).  Must be called before the first log
// message is written.  These are formatted similar to the FormatLogRecord (e.g.
// you can use %D and %T in your header/footer for date and time).
func (w *FileLogWriter) SetHeadFoot(head, foot string) *FileLogWriter {
	w.header, w.trailer = head, foot
	if w.maxlines_curlines == 0 {
		fmt.Fprint(w.file, FormatLogRecord(w.header, &LogRecord{Created: time.Now()}))
	}
	return w
}

// Set rotate at linecount (chainable). Must be called before the first log
// message is written.
func (w *FileLogWriter) SetRotateLines(maxlines int) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateLines: %v\n", maxlines)
	w.maxlines = maxlines
	return w
}

// Set rotate at size (chainable). Must be called before the first log message
// is written.
func (w *FileLogWriter) SetRotateSize(maxsize int) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateSize: %v\n", maxsize)
	w.maxsize = maxsize
	return w
}

// Set rotate daily (chainable). Must be called before the first log message is
// written.
func (w *FileLogWriter) SetRotateDaily(daily bool) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotateDaily: %v\n", daily)
	w.daily = daily
	return w
}

// SetRotate changes whether or not the old logs are kept. (chainable) Must be
// called before the first log message is written.  If rotate is false, the
// files are overwritten; otherwise, they are rotated to another file before the
// new log is opened.
func (w *FileLogWriter) SetRotate(rotate bool) *FileLogWriter {
	//fmt.Fprintf(os.Stderr, "FileLogWriter.SetRotate: %v\n", rotate)
	w.rotate = rotate
	return w
}

// Set the logging format (chainable).  Must be called before the first log
// message is written.
func (w *FileLogWriter) SetMaxFiles(maxFiles int) *FileLogWriter {
	w.maxfiles = maxFiles
	return w
}

// NewXMLLogWriter is a utility method for creating a FileLogWriter set up to
// output XML record log messages instead of line-based ones.
func NewXMLLogWriter(fname string, rotate bool) *FileLogWriter {
	return NewFileLogWriter(fname, rotate).SetFormat(
		`	<record level="%L">
		<timestamp>%D %T</timestamp>
		<source>%S</source>
		<message>%M</message>
	</record>`).SetHeadFoot("<log created=\"%D %T\">", "</log>")
}
