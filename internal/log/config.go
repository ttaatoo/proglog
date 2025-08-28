package log

type Config struct {
	Segment struct {
		// The maximum number of bytes to store in the segment's store file.
		MaxStoreBytes uint64
		// The maximum number of bytes to store in the segment's index file.
		MaxIndexBytes uint64
		// The offset to start at when creating a new segment file.
		// This is used to ensure that each segment file has a unique name.
		InitialOffset uint64
	}
}
