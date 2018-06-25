package com.mapr.fuse;

import jnr.ffi.Pointer;
import lombok.extern.slf4j.Slf4j;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.concurrent.TimeUnit;

import static com.mapr.fuse.Util.getFullPath;

@Slf4j
public class StreamFuse extends FuseStubFS {

    private final static String FAKE_DIR_PATTERN = ".*\\.dir$";
    private final static String FAKE_FILE_PATTERN = ".*\\.dir/.+";

    private final String root;

    private StreamFuse(String root) {
        this.root = root;
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            String mountPoint = args[0];
            String root = args[1];
            StreamFuse stub = new StreamFuse(root);
            stub.mount(Paths.get(mountPoint), true);
            stub.umount();
        } else {
            log.error("Mount point and root dirs don't specified");
            throw new IllegalArgumentException("Specify mount point and root dir paths");
        }
    }

    @Override
    public int getattr(final String path, final FileStat stat) {
        if (isFakeFile(path)) {
            // Get topic info
        } else {
            setupFileAttrs(path, stat);
        }
        return super.getattr(getFullPath(root, path), stat);
    }

    @Override
    public int mkdir(final String path, final long mode) {
        return super.mkdir(getFullPath(root, path), mode);
    }

    @Override
    public int rmdir(final String path) {
        return super.rmdir(getFullPath(root, path));
    }

    @Override
    public int chmod(final String path, final long mode) {
        return super.chmod(getFullPath(root, path), mode);
    }

    @Override
    public int chown(final String path, final long uid, final long gid) {
        return super.chown(getFullPath(root, path), uid, gid);
    }

    @Override
    public int open(final String path, final FuseFileInfo fi) {
        return super.open(getFullPath(root, path), fi);
    }

    @Override
    public int read(final String path, final Pointer buf, final long size, final long offset, final FuseFileInfo fi) {
        return super.read(getFullPath(root, path), buf, size, offset, fi);
    }

    @Override
    public int opendir(final String path, final FuseFileInfo fi) {
        return super.opendir(getFullPath(root, path), fi);
    }

    @Override
    public int readdir(final String path, final Pointer buf, final FuseFillDir filter, final long offset, final FuseFileInfo fi) {
        return super.readdir(getFullPath(root, path), buf, filter, offset, fi);
    }

    @Override
    public int access(final String path, final int mask) {
        return super.access(getFullPath(root, path), mask);
    }

    private void setupFileAttrs(String path, FileStat stat) {
        Path path_ = Paths.get(getFullPath(root, path));
        try {
            BasicFileAttributes basicFileAttributes = Files.readAttributes(path_, BasicFileAttributes.class);
            stat.st_atim.tv_sec.set(basicFileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
            stat.st_ctim.tv_sec.set(basicFileAttributes.creationTime().to(TimeUnit.SECONDS));
            stat.st_mtim.tv_sec.set(basicFileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
            stat.st_gid.set((Number) Files.getAttribute(path_, "unix:gid"));
            stat.st_mode.set((Number) Files.getAttribute(path_, "unix:mode"));
            stat.st_nlink.set((Number) Files.getAttribute(path_, "unix:nlink"));
            stat.st_size.set((Number) Files.getAttribute(path_, "unix:size"));
            stat.st_uid.set((Number) Files.getAttribute(path_, "unix:uid"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private boolean isFakeDir(String path) {
        return path.matches(FAKE_DIR_PATTERN);
    }

    private boolean isFakeFile(String path) {
        return path.matches(FAKE_FILE_PATTERN);
    }
}
