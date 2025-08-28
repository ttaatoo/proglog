package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8 // define the number of bytes used to store the record's length
)

type store struct {
	*os.File
	mu   sync.RWMutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	// get the file size
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// in case we're recreating the store from a file that has existing data
	// which would happen if our service had restarted
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store.
// It returns the number of bytes written and the position of the record in the store.
// The segment will use this position when it creates an associated index entry for this record.
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// write the length of the record first
	// so that when we read the record, we know how many bytes to read
	err = binary.Write(s.buf, enc, uint64(len(p)))
	if err != nil {
		return 0, 0, err
	}

	// write to the buffered writer instead of directly to the file
	// to reduce the number of system calls and improve performance
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	w += lenWidth // add the length of the length field
	s.size += uint64(w)

	return uint64(w), pos, nil
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the buffered writer to the file, in
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// read the length of the size
	length := make([]byte, lenWidth)
	// read the length of the record into buf
	_, err := s.File.ReadAt(length, int64(pos))
	if err != nil {
		return nil, err
	}

	// enc.Uint64 converts the length of the record, which is stored as a byte slice, into a uint64
	// start from pos + lenWidth to read the contents of the record
	b := make([]byte, enc.Uint64(length))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt reads len(p) bytes into p starting at the off offset in the store's file.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
