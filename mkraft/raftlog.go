package mkraft

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

type RaftLogsIface interface {
	GetPrevLogIndexAndTerm() (uint64, uint32)

	AppendLog(commands []byte, term int) error
	AppendLogsInBatch(commandList [][]byte, term int) error
}

func NewRaftLogsImpl(filePath string) RaftLogsIface {
	initLogsLength := 5000
	var file *os.File
	var err error
	if _, statErr := os.Stat(filePath); statErr == nil {
		file, err = os.OpenFile(filePath, os.O_RDWR, 0666)
		if err != nil {
			panic(err)
		}
	} else {
		file, err = os.Create(filePath)
		if err != nil {
			panic(err)
		}
	}

	raftLogs := &SimpleRaftLogsImpl{
		logs:  make([]RaftLogEntry, 0, initLogsLength),
		file:  file,
		mutex: &sync.Mutex{},
	}
	err = raftLogs.load()
	if err != nil {
		panic(fmt.Sprintf("failed to load raft logs: %v", err))
	}
	return raftLogs
}

type RaftLogEntry struct {
	Index    uint64
	Term     uint32
	Commands []byte
}

type SimpleRaftLogsImpl struct {
	logs  []RaftLogEntry
	file  *os.File
	mutex *sync.Mutex
}

const LogMarker byte = '#'

// index starts from 1
func (rl *SimpleRaftLogsImpl) GetPrevLogIndexAndTerm() (uint64, uint32) {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()
	if len(rl.logs) == 0 {
		return 0, 0
	}
	index := len(rl.logs)
	lastLog := rl.logs[index-1]

	return uint64(index), lastLog.Term
}

// write one command to log
func (rl *SimpleRaftLogsImpl) AppendLog(commands []byte, term int) error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	// add in cache
	entry := RaftLogEntry{
		Index:    uint64(len(rl.logs) + 1),
		Term:     uint32(term),
		Commands: commands,
	}
	// serialize: len#term, index, commands#
	buf := rl.serialize(entry)
	_, err := rl.file.Write(buf.Bytes())
	if err != nil {
		return err
	}
	rl.file.Sync() // forced to sync the file to disk
	rl.logs = append(rl.logs, entry)
	return nil
}

func (rl *SimpleRaftLogsImpl) AppendLogsInBatch(commandList [][]byte, term int) error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	var buffers bytes.Buffer
	entries := make([]RaftLogEntry, len(commandList))

	for idx, command := range commandList {
		entry := RaftLogEntry{
			Index:    uint64(len(rl.logs) + 1),
			Term:     uint32(term),
			Commands: command,
		}
		entries[idx] = entry
		// serialize: len#term, index, commands#
		var buf bytes.Buffer = rl.serialize(entry)
		buffers.Write(buf.Bytes())
	}

	rl.file.Sync() // forced to sync the file to disk
	rl.logs = append(rl.logs, entries...)
	return nil
}

func (rl *SimpleRaftLogsImpl) load() error {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	fileInfo, err := rl.file.Stat()
	if err != nil {
		return err
	}

	fileSize := fileInfo.Size()
	if fileSize == 0 {
		return nil
	}

	buf := make([]byte, fileSize)
	_, err = rl.file.Read(buf)
	if err != nil {
		return err
	}

	reader := bytes.NewReader(buf)
	for reader.Len() > 0 {
		entryBuf := make([]byte, 4)
		if _, err := io.ReadFull(reader, entryBuf); err != nil {
			return fmt.Errorf("failed to read entry length: %w", err)
		}
		length := binary.BigEndian.Uint32(entryBuf)

		entryBuf = make([]byte, length)
		if _, err := io.ReadFull(reader, entryBuf); err != nil {
			return fmt.Errorf("failed to read entry: %w", err)
		}

		entry, err := rl.deserialize(entryBuf)
		if err != nil {
			return fmt.Errorf("failed to deserialize entry: %w", err)
		}
		rl.logs = append(rl.logs, entry)
	}
	return nil
}

// [4 bytes: length][1 byte: marker][8 bytes: term][8 bytes: index][N bytes: command][1 byte: marker]
func (rl *SimpleRaftLogsImpl) serialize(entry RaftLogEntry) bytes.Buffer {
	var inner bytes.Buffer
	var full bytes.Buffer

	inner.WriteByte(LogMarker)
	binary.Write(&inner, binary.BigEndian, entry.Term)
	binary.Write(&inner, binary.BigEndian, entry.Index)
	inner.Write(entry.Commands)
	inner.WriteByte(LogMarker)

	length := uint32(inner.Len())
	binary.Write(&full, binary.BigEndian, length)
	full.Write(inner.Bytes())
	return full
}

func (rl *SimpleRaftLogsImpl) deserialize(buf []byte) (RaftLogEntry, error) {
	const (
		termSize   = 4
		indexSize  = 8
		headerSize = 1 + termSize + indexSize
		footerSize = 1
	)

	if len(buf) < headerSize+footerSize {
		return RaftLogEntry{}, fmt.Errorf("buffer too short to contain a log entry")
	}

	if buf[0] != LogMarker || buf[len(buf)-1] != LogMarker {
		return RaftLogEntry{}, fmt.Errorf("invalid log markers: start=%x end=%x", buf[0], buf[len(buf)-1])
	}

	reader := bytes.NewReader(buf[1 : len(buf)-1]) // skip markers

	var term uint32
	if err := binary.Read(reader, binary.BigEndian, &term); err != nil {
		return RaftLogEntry{}, fmt.Errorf("failed to read term: %w", err)
	}

	var index uint64
	if err := binary.Read(reader, binary.BigEndian, &index); err != nil {
		return RaftLogEntry{}, fmt.Errorf("failed to read index: %w", err)
	}

	commands, err := io.ReadAll(reader)
	if err != nil {
		return RaftLogEntry{}, fmt.Errorf("failed to read commands: %w", err)
	}

	return RaftLogEntry{
		Term:     term,
		Index:    index,
		Commands: commands,
	}, nil
}
