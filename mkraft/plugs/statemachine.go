package plugs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

var _ StateMachine = (*stateMachineNoOp)(nil)
var _ StateMachine = (*stateMachineAppendOnly)(nil)

type StateMachine interface {

	// maki: this part is very important, need to discuss with professor and refer to other implementations
	// shall be implemented asynchronosly so that one slow command will not block the whole cluster for client command processing
	ApplyCommand(ctx context.Context, command []byte) ([]byte, error)

	// state machine should be able to ensure the commandList order is well maintained
	// if the command cannot be applied, the error should be encoded in the []byte as binary payload following the statemachine's protocol
	// if the whole command cannot be applied, use the error
	BatchApplyCommand(ctx context.Context, commandList [][]byte) ([][]byte, error)

	Close() error
}

type stateMachineNoOp struct {
}

func NewStateMachineNoOpImpl() *stateMachineNoOp {
	return &stateMachineNoOp{}
}

func (s *stateMachineNoOp) ApplyCommand(ctx context.Context, command []byte) ([]byte, error) {
	// the command shall contain the command index
	return []byte("no op"), nil
}

func (s *stateMachineNoOp) BatchApplyCommand(ctx context.Context, commandList [][]byte) ([][]byte, error) {
	return nil, nil
}

func (s *stateMachineNoOp) Close() error {
	return nil
}

// this is just for verification, so
// the persistent part is not atomically solid
type stateMachineAppendOnly struct {
	sync.RWMutex
	latestAppliedIndex uint64
	dataDirPath        string
	filePath           string
	openFile           *os.File
}

func NewStateMachineAppendOnlyImpl(dataDirPath string) *stateMachineAppendOnly {
	filePath := filepath.Join(dataDirPath, "stateMachine.mk")
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	return &stateMachineAppendOnly{
		dataDirPath: dataDirPath,
		filePath:    filePath,
		openFile:    file,
	}
}

func (s *stateMachineAppendOnly) Close() error {
	return s.openFile.Close()
}

func (s *stateMachineAppendOnly) ApplyCommand(ctx context.Context, command []byte) ([]byte, error) {
	s.Lock()
	defer s.Unlock()
	s.latestAppliedIndex++
	_, err := s.openFile.WriteString(fmt.Sprintf("%d,%s\n", s.latestAppliedIndex, command))
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (s *stateMachineAppendOnly) BatchApplyCommand(ctx context.Context, commandList [][]byte) ([][]byte, error) {
	s.Lock()
	defer s.Unlock()
	for _, command := range commandList {
		_, err := s.openFile.WriteString(fmt.Sprintf("%d,%s\n", s.latestAppliedIndex, command))
		if err != nil {
			return nil, err
		}
	}
	return nil, nil
}
