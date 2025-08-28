package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

/*
*Width define the number of bytes that make up the value in the index

Our index entries contain two fields: the record's offset and its position in the store file.
We store offsets as uint32 and positions as uint64, so they take up 4 and 8 bytes respectively.

We use the entWidth to jump straight to the position of an entry given its offset since the position in the file is
offset * entWidth.
*/

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

// index defines our index file, which comprises a persisted file and a memory-mapped file.
type index struct {
	file *os.File
	// The memory-mapped file that we'll use to access the index
	mmap gommap.MMap
	// The size of the index and where to write the next enrtry appended to the index
	size uint64
}

// newIndex creates an index for the given file.
// We save the current size of the file so we can track the amount of data in the index file as we add index entries.
// we grow the file to the max index size before memory-mapping the file and then return the created index to the caller.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())
	// The reason we resize them now is that, once they're memory-mapped, we can't resize them.
	// We grow the files by appending empty space at the end of them, so the last entry is no longer
	// ad the end of the file - instead, there's some unknown amount of space between this entry and the file's end.
	// This space prevents the service from restarting properly.
	// That's why we shut down the service by truncating the index files to remove the empty space and put the last entry
	// at the end of the file once again.
	if err := os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(idx.file.Fd(),
		// PROT_READ: read-only
		// PROT_WRITE: write-only
		// MAP_SHARED: shared memory
		gommap.PROT_READ|gommap.PROT_WRITE, gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close makes sure the memory-mapped file has synced its data to the persisted file and that the persisted file
// has flushed its contents to stable storage.
// Then it truncates the persisted file to the amount of data that's actually in it.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}

	if err := i.file.Sync(); err != nil {
		return err
	}

	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}

	return i.file.Close()
}

// Read takes in an offset and returns the associated record's position in the store.
// The given offset is relative to the segment's base offset; 0 is always the offset of the index's first entry.
// 1 is the second entry, and so on.
// We use relative offsets to reduce the size of the indexes by storing offsets as uint32s.
// If we used absolute offsets, we'd have to store the offsets as uint64s and require four more bytes for each entry.
func (i *index) Read(offset int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	if offset == -1 {
		// calculate the relative offset of the last entry
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(offset)
	}

	pos = uint64(out) * entWidth
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// size of the offset part of the index entry
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	// size of the position part of the index entry
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

// Write appends the given offset and position to the index.
func (i *index) Write(offset uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	enc.PutUint32(i.mmap[i.size:i.size+offWidth], offset)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += entWidth
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}
