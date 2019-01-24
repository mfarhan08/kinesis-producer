package utils

func CloneByteSlice(data []byte) []byte {
	newSlice := make([]byte, len(data))
	copy(newSlice, data)
	return newSlice
}
