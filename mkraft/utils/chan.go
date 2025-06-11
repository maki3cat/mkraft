package utils

func ReadMultipleFromChannel[T any](ch <-chan T, count int) []T {
	result := make([]T, 0, count)
	for range count {
		select {
		case item, ok := <-ch:
			if !ok {
				return result
			}
			result = append(result, item)
		default:
			return result
		}
	}
	return result
}

func DrainChannel[T any](ch <-chan T, limit int) {
	for range limit {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}
