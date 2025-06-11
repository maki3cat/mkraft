package plugs

import "context"

var _ StateMachine = (*stateMachineNoOp)(nil)

type StateMachine interface {

	// maki: this part is very important, need to discuss with professor and refer to other implementations
	// shall be implemented asynchronosly so that one slow command will not block the whole cluster for client command processing
	ApplyCommand(ctx context.Context, command []byte) ([]byte, error)

	// state machine should be able to ensure the commandList order is well maintained
	// if the command cannot be applied, the error should be encoded in the []byte as binary payload following the statemachine's protocol
	// if the whole command cannot be applied, use the error
	BatchApplyCommand(ctx context.Context, commandList [][]byte) ([][]byte, error)

	GetLatestAppliedIndex() uint64
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

func (s *stateMachineNoOp) GetLatestAppliedIndex() uint64 {
	return 0
}

func (s *stateMachineNoOp) BatchApplyCommand(ctx context.Context, commandList [][]byte) ([][]byte, error) {
	return nil, nil
}
