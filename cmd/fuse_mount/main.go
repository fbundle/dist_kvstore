package main

import (
	"context"
	"flag"
	"log"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Global KV store (filename -> contents)
var (
	kv   = make(map[string][]byte)
	lock sync.RWMutex
)

type KVRoot struct {
	fs.Inode
}

func (r *KVRoot) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	lock.RLock()
	defer lock.RUnlock()

	entries := make([]fuse.DirEntry, 0, len(kv))
	for name := range kv {
		entries = append(entries, fuse.DirEntry{
			Name: name,
			Mode: fuse.S_IFREG,
		})
	}
	return fs.NewListDirStream(entries), 0
}

func (r *KVRoot) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	lock.RLock()
	data, ok := kv[name]
	lock.RUnlock()
	if !ok {
		return nil, syscall.ENOENT
	}

	// Create a child node representing the file
	child := r.NewPersistentInode(
		ctx,
		&KVFile{Name: name, Data: data},
		fs.StableAttr{Mode: syscall.S_IFREG},
	)
	return child, 0
}

func (r *KVRoot) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	lock.Lock()
	kv[name] = []byte{} // create empty file
	lock.Unlock()

	child := r.NewPersistentInode(
		ctx,
		&KVFile{Name: name},
		fs.StableAttr{Mode: syscall.S_IFREG},
	)
	r.AddChild(name, child, false)
	return child, &KVHandle{name}, flags, 0
}

func (r *KVRoot) Unlink(ctx context.Context, name string) syscall.Errno {
	lock.Lock()
	defer lock.Unlock()
	if _, ok := kv[name]; !ok {
		return syscall.ENOENT
	}
	delete(kv, name)
	return 0
}

// ---------- File implementation ----------

type KVFile struct {
	fs.Inode
	Name string
	Data []byte
}

func (f *KVFile) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	lock.RLock()
	defer lock.RUnlock()
	out.Mode = 0644
	out.Size = uint64(len(kv[f.Name]))
	return 0
}

func (f *KVFile) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	return &KVHandle{f.Name}, fuse.FOPEN_KEEP_CACHE, 0
}

func (f *KVFile) Read(ctx context.Context, fh fs.FileHandle, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	lock.RLock()
	defer lock.RUnlock()
	data := kv[f.Name]
	if off >= int64(len(data)) {
		return fuse.ReadResultData(nil), 0
	}
	end := int(off) + len(dest)
	if end > len(data) {
		end = len(data)
	}
	return fuse.ReadResultData(data[off:end]), 0
}

func (f *KVFile) Write(ctx context.Context, fh fs.FileHandle, data []byte, off int64) (uint32, syscall.Errno) {
	lock.Lock()
	defer lock.Unlock()
	buf := kv[f.Name]
	// grow buffer if needed
	newLen := int(off) + len(data)
	if newLen > len(buf) {
		newBuf := make([]byte, newLen)
		copy(newBuf, buf)
		buf = newBuf
	}
	copy(buf[off:], data)
	kv[f.Name] = buf
	return uint32(len(data)), 0
}

// ---------- File handle ----------

type KVHandle struct {
	Name string
}

func main() {
	debug := flag.Bool("debug", false, "print debug data")
	flag.Parse()
	if len(flag.Args()) < 1 {
		log.Fatal("Usage:\n  kvfs MOUNTPOINT")
	}
	mountpoint := flag.Arg(0)

	opts := &fs.Options{
		MountOptions: fuse.MountOptions{
			Debug: *debug,
		},
	}
	server, err := fs.Mount(mountpoint, &KVRoot{}, opts)
	if err != nil {
		log.Fatalf("Mount fail: %v\n", err)
	}
	log.Printf("Mounted at %s\n", mountpoint)
	server.Wait()
}
