package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/ttaaoo/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

/*
The segment wraps the index and store types to coordinate operations across the two.
For example, when the log appends a record to the active segment, the segment needs to write
the data to its store and add a new entry in the index.
*/

type segment struct {
	store *store
	index *index
	// the (global) starting offset of the segment
	baseOffset uint64
	// the next (global) offset to write to the segment
	nextOffset uint64
	config     Config
}

// The log calls newSegment when it needs to add a new segment, such as when the current active segment
// hits its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	// open or create the store file
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// set the segment's next offset to prepare for the next appended record.
	// If the index is empty, then the next record appended to the segment would be the first record.
	if off, _, err := s.index.Read(-1); err != nil {
		s.nextOffset = baseOffset
	} else {
		// e.g. baseOffset = 10, off = 10, then the next record appended to the segment would be the 21st record.
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append writes the record to the segment and returns the newly appended record's offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	if err = s.index.Write(
		// index offsets are relative to base offset
		uint32(s.nextOffset-uint64(s.baseOffset)),
		pos,
	); err != nil {
		return 0, err
	}
	s.nextOffset++
	return cur, nil
}

// Read returns the record for the given offset.
// Similar to writes, to read a record the segment must first translate the absolute index into a relative index
// and get the associated index entry.
func (s *segment) Read(off uint64) (*api.Record, error) {
	indexOffset := int64(off - s.baseOffset)
	// from the index, we get the position of the record in the store
	_, pos, err := s.index.Read(indexOffset)
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

// The log uses this method to know it needs to create a new segment.
// if you wrote a small number of long logs, then you'd hit the segment bytes limit.
// if you wrote a large number of short logs, then you'd hit the index bytes limit.
func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes || s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove removes the segment and all its files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}

	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	return os.Remove(s.store.Name())
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// returns the nearest and lesser multiple of k in j
// e.g. nearestMultiple(9,4) = 8
// We take the lesser multiple to make sure we stay under the user's disk capacity.
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}

	return ((j - k + 1) / k) * k
}
