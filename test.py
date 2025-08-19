import os; os.environ["FUSE_LIBRARY_PATH"] = "/home/khanh/ws/miniforge3/envs/test/lib/libfuse3.so"


import llfuse, os, stat, errno, time
from llfuse import FUSEError

class HelloFS(llfuse.Operations):
    def getattr(self, inode, ctx=None):
        entry = llfuse.EntryAttributes()
        entry.st_ino = inode
        entry.st_mode = stat.S_IFDIR | 0o755 if inode == 1 else stat.S_IFREG | 0o444
        entry.st_nlink = 2 if inode == 1 else 1
        entry.st_size = 0 if inode == 1 else len(b"Hello World!\n")
        now = int(time.time()*1e9)
        entry.st_atime_ns = entry.st_mtime_ns = entry.st_ctime_ns = now
        return entry

    def lookup(self, parent_inode, name, ctx=None):
        if parent_inode == 1 and name == b"hello.txt":
            return self.getattr(2)
        raise FUSEError(errno.ENOENT)

    def readdir(self, inode, off):
        if inode != 1: return
        yield llfuse.Direntry(b'.', 1)
        yield llfuse.Direntry(b'..', 1)
        yield llfuse.Direntry(b'hello.txt', 2)

    def open(self, inode, flags, ctx):
        if inode != 2: raise FUSEError(errno.ENOENT)
        return inode

    def read(self, fh, off, size):
        data = b"Hello World!\n"
        return data[off:off+size]

if __name__ == "__main__":
    mountpoint = "./mnt"
    os.makedirs(mountpoint, exist_ok=True)
    ops = HelloFS()
    llfuse.init(ops, mountpoint, [])
    try:
        llfuse.main()
    finally:
        llfuse.close()
