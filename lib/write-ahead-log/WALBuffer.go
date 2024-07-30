package writeaheadlog

const MINIMAL_ENTRY_SIZE = 31

type WALBuffer struct {
	Data []WriteAheadLogEntry
	Size int // combined size of all entries in the buffer
}

func NewWALBuffer(segmentSize int) *WALBuffer {
	data := make([]WriteAheadLogEntry, ceilDiv(segmentSize, MINIMAL_ENTRY_SIZE))
	return &WALBuffer{
		Data: data,
		Size: 0,
	}
}

func (buffer *WALBuffer) Add(entry WriteAheadLogEntry) {
	buffer.Data = append(buffer.Data, entry)
	buffer.Size += entry.Size
}

// helper function to calculate the ceiling of a division
func ceilDiv(a, b int) int {
	return (a + b - 1) / b
}
