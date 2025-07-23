package common

func NonBlockingSend(ch chan<- struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func BroadcastNonBlocking(triggerChans []chan struct{}) {
	for _, ch := range triggerChans {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func DrainChannel(ch <-chan struct{}) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}
