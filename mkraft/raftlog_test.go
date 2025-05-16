package mkraft

import (
	"os"
	"path/filepath"
	"testing"
)

// Helper to create a temp file and cleanup
func withTempLogFile(t *testing.T, testFunc func(path string)) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "raftlog_test.log")
	testFunc(path)
}

func TestAppendLogAndGetPrevLogIndexAndTerm(t *testing.T) {
	withTempLogFile(t, func(path string) {
		logs := NewRaftLogsImpl(path)
		prevIdx, prevTerm := logs.GetPrevLogIndexAndTerm()
		if prevIdx != 0 || prevTerm != 0 {
			t.Fatalf("expected empty log, got idx=%d term=%d", prevIdx, prevTerm)
		}

		err := logs.AppendLog([]byte("cmd1"), 1)
		if err != nil {
			t.Fatalf("AppendLog failed: %v", err)
		}

		idx, term := logs.GetPrevLogIndexAndTerm()
		if idx != 1 || term != 1 {
			t.Fatalf("expected idx=1 term=1, got idx=%d term=%d", idx, term)
		}

		err = logs.AppendLog([]byte("cmd2"), 2)
		if err != nil {
			t.Fatalf("AppendLog failed: %v", err)
		}

		idx, term = logs.GetPrevLogIndexAndTerm()
		if idx != 2 || term != 2 {
			t.Fatalf("expected idx=2 term=2, got idx=%d term=%d", idx, term)
		}
	})
}

func TestAppendLogsInBatch(t *testing.T) {
	withTempLogFile(t, func(path string) {
		logs := NewRaftLogsImpl(path)
		cmds := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		err := logs.AppendLogsInBatch(cmds, 5)
		if err != nil {
			t.Fatalf("AppendLogsInBatch failed: %v", err)
		}
		idx, term := logs.GetPrevLogIndexAndTerm()
		if idx != 3 || term != 5 {
			t.Fatalf("expected idx=3 term=5, got idx=%d term=%d", idx, term)
		}
	})
}

func TestPersistenceAndLoad(t *testing.T) {
	withTempLogFile(t, func(path string) {
		// Write logs
		logs := NewRaftLogsImpl(path)
		logs.AppendLog([]byte("persist1"), 10)
		logs.AppendLog([]byte("persist2"), 11)

		// Close file to flush
		if impl, ok := logs.(*SimpleRaftLogsImpl); ok {
			impl.file.Close()
		}

		// Reopen and reload
		logs2 := NewRaftLogsImpl(path)
		idx, term := logs2.GetPrevLogIndexAndTerm()
		if idx != 2 || term != 11 {
			t.Fatalf("expected idx=2 term=11 after reload, got idx=%d term=%d", idx, term)
		}
	})
}

func TestAppendLogConcurrencySafety(t *testing.T) {
	withTempLogFile(t, func(path string) {
		logs := NewRaftLogsImpl(path)
		const n = 20
		errs := make(chan error, n)
		done := make(chan struct{})
		for i := 0; i < n; i++ {
			go func(i int) {
				err := logs.AppendLog([]byte{byte(i)}, i)
				errs <- err
			}(i)
		}
		for i := 0; i < n; i++ {
			if err := <-errs; err != nil {
				t.Errorf("AppendLog failed: %v", err)
			}
		}
		close(done)
		idx, _ := logs.GetPrevLogIndexAndTerm()
		if idx != n {
			t.Fatalf("expected idx=%d, got %d", n, idx)
		}
	})
}

func TestSerializeDeserialize(t *testing.T) {
	entry := RaftLogEntry{
		Index:    42,
		Term:     7,
		Commands: []byte("hello"),
	}
	impl := &SimpleRaftLogsImpl{}
	buf := impl.serialize(entry)
	got, err := impl.deserialize(buf.Bytes()[4:]) // skip length prefix
	if err != nil {
		t.Fatalf("deserialize failed: %v", err)
	}
	if got.Index != entry.Index || got.Term != entry.Term || string(got.Commands) != string(entry.Commands) {
		t.Fatalf("deserialize mismatch: got %+v, want %+v", got, entry)
	}
}

func TestDeserializeInvalidBuffer(t *testing.T) {
	impl := &SimpleRaftLogsImpl{}
	_, err := impl.deserialize([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short buffer")
	}
	// Invalid marker
	buf := []byte("invalidbuffer")
	_, err = impl.deserialize(buf)
	if err == nil {
		t.Fatal("expected error for invalid marker")
	}
}

func TestLoadEmptyFile(t *testing.T) {
	withTempLogFile(t, func(path string) {
		file, err := os.Create(path)
		if err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
		file.Close()
		_ = NewRaftLogsImpl(path) // should not panic
	})
}
