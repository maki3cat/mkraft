package persister

import (
	"context"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func setupTest() (RaftLogs, func()) {
	raftLog := NewRaftLogsImplAndLoad("./tmp/", zap.NewNop(), NewRaftSerdeImpl())
	cleanup := func() {
		os.RemoveAll("./tmp/")
	}
	return raftLog, cleanup
}

func TestRaftLog_InitFromLogFile(t *testing.T) {
	tests := []struct {
		name          string
		setupFile     func(string) []*RaftLogEntry
		wantErr       bool
		wantLogsEmpty bool
	}{
		{
			name:          "new file created if doesn't exist",
			setupFile:     nil,
			wantErr:       false,
			wantLogsEmpty: true,
		},
		{
			name: "existing empty file loads successfully",
			setupFile: func(path string) []*RaftLogEntry {
				f, _ := os.Create(path)
				f.Close()
				return nil
			},
			wantErr:       false,
			wantLogsEmpty: true,
		},
		{
			name: "corrupted file doesn't returns error",
			setupFile: func(path string) []*RaftLogEntry {
				f, _ := os.Create(path)
				f.Write([]byte("corrupted data"))
				f.Close()
				return nil
			},
			wantErr:       false,
			wantLogsEmpty: true,
		},
		{
			name: "valid log entries are loaded correctly",
			setupFile: func(path string) []*RaftLogEntry {
				f, _ := os.Create(path)
				defer f.Close()

				// Create test log entries
				entries := []*RaftLogEntry{
					{Term: 1, Commands: []byte("cmd1")},
					{Term: 1, Commands: []byte("cmd2")},
					{Term: 2, Commands: []byte("cmd3")},
				}

				// Write entries using the same format as production code
				serde := NewRaftSerdeImpl()
				batchSeparator := byte('\x1D')

				for _, entry := range entries {
					// Write separator first
					f.Write([]byte{batchSeparator})

					// Serialize and write entry
					data, _ := serde.BatchSerialize([]*RaftLogEntry{entry})
					f.Write(data)
				}

				return entries
			},
			wantErr:       false,
			wantLogsEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			testFile := "test_init.log"
			defer os.Remove(testFile)

			var expectedLogs []*RaftLogEntry
			if tt.setupFile != nil {
				expectedLogs = tt.setupFile(testFile)
			}

			rl := &raftLogs{
				file:           nil,
				mutex:          &sync.Mutex{},
				batchSeparater: '\x1D',
				batchSize:      1024 * 8,
				serde:          NewRaftSerdeImpl(),
				logger:         zap.NewNop(),
			}

			// Open the file
			file, err := os.OpenFile(testFile, os.O_RDWR|os.O_CREATE, 0666)
			if err != nil {
				t.Fatal(err)
			}
			rl.file = file
			defer file.Close()

			err = rl.initFromLogFile()

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}

			if tt.wantLogsEmpty {
				assert.Empty(t, rl.logs)
			} else {
				assert.NotEmpty(t, rl.logs)
				assert.Equal(t, len(expectedLogs), len(rl.logs))

				// Verify each log entry matches
				for i, expected := range expectedLogs {
					assert.Equal(t, expected.Term, rl.logs[i].Term)
					assert.Equal(t, expected.Commands, rl.logs[i].Commands)
				}
			}
		})
	}
}

func TestRaftLog_AppendLogsInBatch(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	tests := []struct {
		name        string
		commands    [][]byte
		term        uint32
		wantErr     bool
		wantLastIdx uint64
	}{
		{
			name:        "append single log",
			commands:    [][]byte{[]byte("log1")},
			term:        1,
			wantErr:     false,
			wantLastIdx: 1,
		},
		{
			name:        "append multiple logs",
			commands:    [][]byte{[]byte("log2"), []byte("log3")},
			term:        1,
			wantErr:     false,
			wantLastIdx: 3,
		},
		{
			name:        "append empty batch",
			commands:    [][]byte{},
			term:        1,
			wantErr:     false,
			wantLastIdx: 3, // should remain unchanged
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := raftLog.AppendLogsInBatch(context.Background(), tt.commands, tt.term)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantLastIdx, raftLog.GetLastLogIdx())
			}
		})
	}
}

func TestRaftLog_UpdateLogsInBatch(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	// Setup initial logs
	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2")}, 1)
	assert.NoError(t, err)

	tests := []struct {
		name        string
		preLogIndex uint64
		commands    [][]byte
		term        uint32
		wantErr     bool
		wantLastIdx uint64
	}{
		{
			name:        "update existing log",
			preLogIndex: 1,
			commands:    [][]byte{[]byte("newlog1")},
			term:        1,
			wantErr:     false,
			wantLastIdx: 2,
		},
		{
			name:        "update with term mismatch",
			preLogIndex: 1,
			commands:    [][]byte{[]byte("newlog2")},
			term:        2,
			wantErr:     true,
			wantLastIdx: 2,
		},
		{
			name:        "update with invalid index",
			preLogIndex: 5,
			commands:    [][]byte{[]byte("newlog3")},
			term:        1,
			wantErr:     true,
			wantLastIdx: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := raftLog.UpdateLogsInBatch(context.Background(), tt.preLogIndex, tt.commands, tt.term)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tt.wantLastIdx, raftLog.GetLastLogIdx())
		})
	}
}

func TestRaftLogs_GetTermByIndex(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	// Setup initial logs
	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2")}, 1)
	assert.NoError(t, err)

	tests := []struct {
		name      string
		index     uint64
		wantTerm  uint32
		wantError bool
	}{
		{
			name:      "get term for first log",
			index:     1,
			wantTerm:  1,
			wantError: false,
		},
		{
			name:      "get term for second log",
			index:     2,
			wantTerm:  1,
			wantError: false,
		},
		{
			name:      "get term for index 0",
			index:     0,
			wantTerm:  0,
			wantError: false,
		},
		{
			name:      "get term for invalid index",
			index:     5,
			wantTerm:  0,
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term, err := raftLog.GetTermByIndex(tt.index)
			if tt.wantError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.wantTerm, term)
			}
		})
	}
}

func TestRaftLogs_GetLastLogIdxAndTerm(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	// Test empty log
	idx, term := raftLog.GetLastLogIdxAndTerm()
	assert.Equal(t, uint64(0), idx)
	assert.Equal(t, uint32(0), term)

	// Add some logs
	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1")}, 1)
	assert.NoError(t, err)
	idx, term = raftLog.GetLastLogIdxAndTerm()
	assert.Equal(t, uint64(1), idx)
	assert.Equal(t, uint32(1), term)

	// Add more logs with different term
	err = raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log2")}, 2)
	assert.NoError(t, err)
	idx, term = raftLog.GetLastLogIdxAndTerm()
	assert.Equal(t, uint64(2), idx)
	assert.Equal(t, uint32(2), term)
}

// func TestRaftLogs_ReadLogsInBatchFromIdx(t *testing.T) {
// 	raftLog, cleanup := setupTest()
// 	defer cleanup()

// 	// Setup initial logs
// 	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3")}, 1)
// 	assert.NoError(t, err)

// 	tests := []struct {
// 		name        string
// 		startIdx    uint64
// 		wantNumLogs int
// 		wantError   bool
// 	}{
// 		{
// 			name:        "read from start",
// 			startIdx:    1,
// 			wantNumLogs: 3,
// 			wantError:   false,
// 		},
// 		{
// 			name:        "read from middle",
// 			startIdx:    2,
// 			wantNumLogs: 2,
// 			wantError:   false,
// 		},
// 		{
// 			name:        "read from last",
// 			startIdx:    3,
// 			wantNumLogs: 1,
// 			wantError:   false,
// 		},
// 		{
// 			name:        "read from invalid index",
// 			startIdx:    4,
// 			wantNumLogs: 0,
// 			wantError:   true,
// 		},
// 		{
// 			name:        "read from index 0",
// 			startIdx:    0,
// 			wantNumLogs: 0,
// 			wantError:   true,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			logs, err := raftLog.ReadLogsInBatchFromIdx(tt.startIdx)
// 			if tt.wantError {
// 				assert.Error(t, err)
// 			} else {
// 				assert.NoError(t, err)
// 				assert.Equal(t, tt.wantNumLogs, len(logs))
// 			}
// 		})
// 	}
// }

func TestRaftLogs_CheckPreLog(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	// Setup initial logs
	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2")}, 1)
	assert.NoError(t, err)
	err = raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log3")}, 2)
	assert.NoError(t, err)

	tests := []struct {
		name        string
		preLogIndex uint64
		term        uint32
		want        bool
	}{
		{
			name:        "matching index and term",
			preLogIndex: 3,
			term:        2,
			want:        true,
		},
		{
			name:        "matching index wrong term",
			preLogIndex: 3,
			term:        1,
			want:        false,
		},
		{
			name:        "wrong index",
			preLogIndex: 2,
			term:        2,
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := raftLog.CheckPreLog(tt.preLogIndex, tt.term)
			assert.Equal(t, tt.want, got)
		})
	}
}
