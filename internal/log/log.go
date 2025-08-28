package log

import (
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	api "github.com/ttaaoo/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex

	// The directory the log will store its segments in.
	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

/*
When a log starts, it's responsible for setting itself up
for the segments that already exist on disk or, if the log is new and has no segments,
for bootstrapping the initial segment.

We fetch the list of the segments on disk, parse and sort the based offsets (because we want
our slice of segments to be in order from oldest to newest), and then create the segments with the newSegment() helper
method, which creates a segment for the base offset you pass in.
*/
func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}

	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// setup is a helper method that initializes the log from the segments on disk.
// it creates the segments and sets the active segment.
func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// sort the base offsets in ascending order
	slices.SortFunc(baseOffsets, func(i, j uint64) int {
		return int(i - j)
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// baseOffset contains dup for index and store files so we skip the dup
		i++
	}

	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	l.activeSegment = l.segments[len(l.segments)-1]
	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(offset uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var s *segment
	for _, segment := range l.segments {
		if segment.baseOffset <= offset && offset < segment.nextOffset {
			s = segment
			break
		}
	}
	if s == nil || s.nextOffset <= offset {
		return nil, api.ErrOffsetOutOfRange{Offset: offset}
	}

	return s.Read(offset)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) newSegment(offset uint64) error {
	s, err := newSegment(l.Dir, offset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}

	return off - 1, nil
}

// Truncate removes all segments whose highest offset is lower than lowest.
// Because we don't have disks with infinite space, we'll periodically call Truncate()
// to remove old segments and free up space.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}

	l.segments = segments
	l.activeSegment = l.segments[len(l.segments)-1]
	return nil
}

// Reader returns an io.Reader to read the whole log.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}

	return io.MultiReader(readers...)
}

// originReader is a wrapper around a store that implements the io.Reader interface.
// It reads from the store at the current offset and increments the offset after each read.
type originReader struct {
	*store
	offset int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.offset)
	o.offset += int64(n)
	return n, err
}

func (o *originReader) Close() error {
	return nil
}
