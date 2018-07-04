package com.mapr.fuse;

import jnr.ffi.Pointer;
import lombok.extern.slf4j.Slf4j;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class StreamFuse extends FuseStubFS {

    private final static String FAKE_DIR_PATTERN = ".*\\.dir$";
    private final static String FAKE_FILE_PATTERN = ".*\\.dir/.+";

    private final Path root;

    private StreamFuse(Path root) {
        this.root = root;
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            String mountPoint = args[0];
            String root = args[1];

            log.info("Mount point -> {}", mountPoint);
            log.info("Root folder -> {}", root);

            StreamFuse stub = new StreamFuse(Paths.get(root));
            stub.mount(Paths.get(mountPoint), true);
            stub.umount();
        } else {
            log.error("Mount point and root dirs don't specified");
            throw new IllegalArgumentException("Specify mount point and root dir paths");
        }
    }

    private static Path getFullPath(Path root, String partial) {
        if (partial.startsWith("/")) {
            partial = partial.substring(1);
        }
        return root.resolve(partial);
    }

    @Override
    public int getattr(final String path, final FileStat stat) {
        String fullPath = getFullPath(root, path).toString();
        log.info("Get attr for -> {}", fullPath);
        if (isFakeDir(path)) {
            // Get stream info
        } else if ((isFakeFile(fullPath))) {
            // Get topic info
        } else {
            if (Files.exists(Paths.get(fullPath))) {
                setupAttrs(path, stat);
            }
        }
        return super.getattr(path, stat);
    }

    @Override
    public int mkdir(final String path, final long mode) {
        String fullPath = getFullPath(root, path).toString();
        log.info("mkdir for -> {}", fullPath);
        return super.mkdir(fullPath, mode);
    }

    @Override
    public int rmdir(final String path) {
        String fullPath = getFullPath(root, path).toString();
        log.info("rmdir for -> {}", fullPath);
        return super.rmdir(fullPath);
    }

    @Override
    public int chmod(final String path, final long mode) {
        String fullPath = getFullPath(root, path).toString();
        log.info("chmod for -> {}", fullPath);
        return super.chmod(fullPath, mode);
    }

    @Override
    public int chown(final String path, final long uid, final long gid) {
        String fullPath = getFullPath(root, path).toString();
        log.info("chown for -> {}", fullPath);
        return super.chown(fullPath, uid, gid);
    }

    @Override
    public int open(final String path, final FuseFileInfo fi) {
        String fullPath = getFullPath(root, path).toString();
        log.info("open for -> {}", fullPath);
        return super.open(fullPath, fi);
    }

    @Override
    public int read(final String path, final Pointer buf, final long size, final long offset, final FuseFileInfo fi) {
        String fullPath = getFullPath(root, path).toString();
        if (isFakeFile(fullPath)) {
            return 0;
        } else {
            byte[] batchOfBytes = new byte[(int) size];
            int numOfReadBytes = readPartOfFile(fullPath, batchOfBytes, (int) offset, (int) size);
            buf.put(0, batchOfBytes, 0, numOfReadBytes);
            return numOfReadBytes;
        }
    }

    /**
     * @param fullPath     path to the file
     * @param batchOfBytes the buffer into which the data is read.
     * @param offset       the start offset in the destination array
     * @param size         the maximum number of bytes read.
     * @return the total number of bytes read into the buffer
     */
    private int readPartOfFile(String fullPath, byte[] batchOfBytes, int offset, int size) {
        int numOfReadBytes;
        try (FileInputStream fis = new FileInputStream(fullPath)) {
            fis.skip(offset);
            numOfReadBytes = fis.read(batchOfBytes, 0, size);
        } catch (IOException e) {
            log.error("Problems with reading file");
            throw new RuntimeException(e);
        }
        return numOfReadBytes;
    }

    @Override
    public int opendir(final String path, final FuseFileInfo fi) {
        String fullPath = getFullPath(root, path).toString();
        log.info("opendir for -> {}", fullPath);
        return super.opendir(fullPath, fi);
    }

    @Override
    public int readdir(final String path, final Pointer buf, final FuseFillDir filter, final long offset, final FuseFileInfo fi) {
        String fullPath = getFullPath(root, path).toString();
        log.info("readdir for -> {}", fullPath);
        File file = new File(fullPath);

        if (file.isDirectory()) {
            filter.apply(buf, ".", null, 0);
            filter.apply(buf, "..", null, 0);

            Arrays.stream(Objects.requireNonNull(file.listFiles()))
                    .map(File::getName)
                    .forEach(x -> filter.apply(buf, x, null, 0));
        }
        return 0;
    }

    @Override
    public int access(final String path, final int mask) {
        String fullPath = getFullPath(root, path).toString();
        return super.access(fullPath, mask);
    }

    private void setupAttrs(String path, FileStat stat) {
        Path fullPath = getFullPath(root, path);
        try {
            BasicFileAttributes basicFileAttributes = Files.readAttributes(fullPath, BasicFileAttributes.class);
            stat.st_atim.tv_sec.set(basicFileAttributes.lastAccessTime().to(TimeUnit.SECONDS));
            stat.st_ctim.tv_sec.set(basicFileAttributes.creationTime().to(TimeUnit.SECONDS));
            stat.st_mtim.tv_sec.set(basicFileAttributes.lastModifiedTime().to(TimeUnit.SECONDS));
            stat.st_gid.set((Number) Files.getAttribute(fullPath, "unix:gid"));
            stat.st_mode.set((Number) Files.getAttribute(fullPath, "unix:mode"));
            stat.st_nlink.set((Number) Files.getAttribute(fullPath, "unix:nlink"));
            stat.st_size.set((Number) Files.getAttribute(fullPath, "unix:size"));
            stat.st_uid.set((Number) Files.getAttribute(fullPath, "unix:uid"));
        } catch (IOException e) {
            log.error("Problems with reading file/dir attributes");
            throw new RuntimeException(e);
        }
    }

    private boolean isFakeDir(String path) {
        return path.matches(FAKE_DIR_PATTERN);
    }

    private boolean isFakeFile(String path) {
        return path.matches(FAKE_FILE_PATTERN);
    }
}
