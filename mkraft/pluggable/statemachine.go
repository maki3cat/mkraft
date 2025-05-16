package pluggable

var _ StateMachineIface = (*StateMachineNoOpImpl)(nil)

type StateMachineIface interface {
	ApplyCommand(command []byte) ([]byte, error)
}

type StateMachineNoOpImpl struct {
}

func NewStateMachineNoOpImpl() *StateMachineNoOpImpl {
	return &StateMachineNoOpImpl{}
}

func (s *StateMachineNoOpImpl) ApplyCommand(command []byte) ([]byte, error) {
	return []byte("no op"), nil
}
