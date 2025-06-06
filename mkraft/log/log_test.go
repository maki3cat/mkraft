package log

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// Complex-Case-1: test atomicity, and handling of corrupt writes
// corrupt writes are made by truncating the file to a partial length, and then writing again
func TestRaftLogs_Atomicity(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	partialLen := 3

	// step-1: write log1, log2, log3, log4, log5; and then truncate the file to 3 bytes
	err := raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3"), []byte("log4"), []byte("log5")}, 1)
	assert.NoError(t, err)

	// Type assert to access internal file field
	impl := raftLog.(*WALInspiredRaftLogsImpl)
	impl.file.Truncate(int64(partialLen))
	impl.file.Sync()
	fi, err := impl.file.Stat()
	assert.NoError(t, err)
	assert.Equal(t, int64(partialLen), fi.Size())
	// fmt.Println("file size after truncate", fi.Size())

	// step-2: write log1, log2, log3, log4, log5; again;
	err = raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3"), []byte("log4"), []byte("log5")}, 1)
	assert.NoError(t, err)
	fi, err = impl.file.Stat()
	assert.NoError(t, err)
	assert.Less(t, int64(partialLen), fi.Size())
	// fmt.Println("file size after retry appending", fi.Size())
	impl.file.Close()

	// step-3: restart the raftLog, should only get 5 logs;
	raftLog = NewRaftLogsImplAndLoad("test.log", zap.NewNop(), NewRaftSerdeImpl())
	impl = raftLog.(*WALInspiredRaftLogsImpl)
	// fmt.Println("len(impl.logs)", len(impl.logs))
	assert.Equal(t, uint64(5), impl.GetLastLogIdx())
	entries, err := raftLog.ReadLogsInBatchFromIdx(1)
	assert.NoError(t, err)
	assert.Equal(t, 5, len(entries))
}

// Case-2: test race conditions
// test case: write logs in parallel, and check if the log sequence shall be correct
func TestRaftLogs_RaceConditions(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()

	go func() {
		raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3"), []byte("log4"), []byte("log5")}, 1)
	}()
	time.Sleep(1 * time.Millisecond)

	go func() {
		raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log6"), []byte("log7"), []byte("log8"), []byte("log9"), []byte("log10")}, 1)
	}()
	time.Sleep(1 * time.Millisecond)

	entries, err := raftLog.ReadLogsInBatchFromIdx(1)
	assert.NoError(t, err)
	assert.Equal(t, 10, len(entries))
	// the log shall be log1, log2, log3, log4, log5, log10, log12, log13, log14, log15
	for i := range entries {
		// fmt.Println("entry", i, string(entry.Commands))
		assert.Equal(t, []byte("log"+strconv.Itoa(i+1)), entries[i].Commands)
	}
}

// Case-3: test update with overwrite and without no overwrite
// test case: update the log with overwrite, and check if the log sequence shall be correct
func TestRaftLogs_UpdateWithOverwrite(t *testing.T) {
	raftLog, cleanup := setupTest()
	defer cleanup()
	raftLog.AppendLogsInBatch(context.Background(), [][]byte{[]byte("log1"), []byte("log2"), []byte("log3"), []byte("log4"), []byte("log5")}, 1)

	raftLog.UpdateLogsInBatch(context.Background(), 2, [][]byte{[]byte("log10"), []byte("log12")}, 1)
	entries, err := raftLog.ReadLogsInBatchFromIdx(1)
	assert.NoError(t, err)
	assert.Equal(t, 4, len(entries))

	assert.Equal(t, []byte("log1"), entries[0].Commands)
	assert.Equal(t, []byte("log2"), entries[1].Commands)
	assert.Equal(t, []byte("log10"), entries[2].Commands)
	assert.Equal(t, []byte("log12"), entries[3].Commands)
}

