package com.mapr.fuse;

import com.mapr.fuse.client.TopicReader;
import com.mapr.fuse.service.AdminTopicService;
import com.mapr.fuse.service.TopicDataService;
import jnr.ffi.Pointer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import ru.serce.jnrfuse.ErrorCodes;
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

import static ru.serce.jnrfuse.struct.FileStat.S_IFDIR;
import static ru.serce.jnrfuse.struct.FileStat.S_IFREG;
import static ru.serce.jnrfuse.struct.FileStat.S_IRUSR;

@Slf4j
public class StreamFuse extends FuseStubFS {

    private final static String STREAM_PATTERN = ".*\\.st$";
    private final static String TOPIC_PATTERN = ".*\\.st/[^/]+$";
    private final static String PARTITION_PATTERN = ".*\\.st/[^/]+/.+";

    private final static String TOPIC_NAME_PATTERN = "/%s:%s";

    private final Path root;
    private TopicDataService tdService;
    private AdminTopicService adminService;

    private StreamFuse(Path root, TopicDataService tdService, AdminTopicService adminService) {
        this.root = root;
        this.tdService = tdService;
        this.adminService = adminService;
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            String mountPoint = args[0];
            String root = args[1];

            log.info("Mount point -> {}", mountPoint);
            log.info("Root folder -> {}", root);

            TopicReader reader = new TopicReader();
            TopicDataService topicDataService = new TopicDataService(reader);
            AdminTopicService adminService = new AdminTopicService(new Configuration());
            StreamFuse stub = new StreamFuse(Paths.get(root), topicDataService, adminService);
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
        int res = 0;
        Path fullPath = getFullPath(root, path);
        log.info("Get attr for -> {}", fullPath);

        if (isMatchPattern(fullPath, TOPIC_PATTERN) && isTopicExist(fullPath)) {
            setupAttrs(getStreamName(fullPath.getParent()), stat);
            stat.st_mode.set(S_IFDIR + S_IRUSR);
        } else if (isMatchPattern(fullPath, PARTITION_PATTERN) &&
                isPartitionExist(fullPath.getParent(), Integer.parseInt(fullPath.getFileName().toString()))) {
            setupAttrs(getStreamName(fullPath.getParent().getParent()), stat);
            stat.st_mode.set(S_IFREG + S_IRUSR);
            stat.st_nlink.set(1);
            stat.st_size.set(getPartitionSize(fullPath));
        } else {
            if (Files.exists(fullPath)) {
                setupAttrs(path, stat);
            } else {
                res = -ErrorCodes.ENOENT();
            }
        }
        return res;
    }

    private int getPartitionSize(Path fullPath) {
        return tdService.requestTopicSizeData(transformToTopicName(fullPath.getParent()),
                getPartitionId(fullPath)).stream()
                .mapToInt(Integer::intValue)
                .sum();
    }

    private boolean isPartitionExist(Path path, Integer partitionId) {
        return adminService.getTopicPartitions(getStreamName(path.getParent()), getTopicName(path)) > partitionId;
    }

    private boolean isTopicExist(Path fullPath) {
        return adminService.getTopicNames(getStreamName(fullPath.getParent())).contains(getTopicName(fullPath));
    }

    private String getTopicName(Path fullPath) {
        return fullPath.getFileName().toString();
    }

    private String transformToTopicName(Path fullPath) {
        return String.format(TOPIC_NAME_PATTERN,
                fullPath.getParent().getFileName().toString(),
                fullPath.getFileName().toString());
    }

    @Override
    @SneakyThrows
    public int mkdir(final String path, final long mode) {
        Path fullPath = getFullPath(root, path);
        log.info("mkdir for -> {}", fullPath);
        if (isMatchPattern(fullPath, STREAM_PATTERN)) {
            Files.createDirectory(fullPath);
            adminService.createStream(path);
        } else if (isMatchPattern(fullPath, TOPIC_PATTERN)) {
            adminService.createTopic(getStreamName(fullPath.getParent()), fullPath.getFileName().toString());
        } else if (isMatchPattern(fullPath, PARTITION_PATTERN)) {
            return -1;
        } else {
            Files.createDirectory(fullPath);
        }
        return 0;
    }

    private String getStreamName(Path fullPath) {
        return String.format("/%s", fullPath.getFileName().toString());
    }

    @Override
    @SneakyThrows
    public int rmdir(final String path) {
        Path fullPath = getFullPath(root, path);
        log.info("rmdir for -> {}", fullPath);
        if (isMatchPattern(fullPath, STREAM_PATTERN)) {
            Files.deleteIfExists(fullPath);
            adminService.removeStream(getStreamName(fullPath));
        } else if (isMatchPattern(fullPath, TOPIC_PATTERN)) {
            adminService.removeTopic(getStreamName(fullPath.getParent()),
                    fullPath.getFileName().toString());
        }
        Files.deleteIfExists(fullPath);
        return 0;
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
        Path fullPath = getFullPath(root, path);
        log.info("read for -> {}", fullPath);
        if (isMatchPattern(fullPath, PARTITION_PATTERN)) {
            long amountOfBytes = offset + size;
            TopicPartition partition = new TopicPartition(transformToTopicName(fullPath.getParent()), getPartitionId(fullPath));
            byte[] vr = tdService.readRequiredBytesFromTopicPartition(partition, offset, amountOfBytes, 2000L);
            buf.put(0, vr, 0, vr.length);
            return vr.length;
        } else {
            log.info("read NORMAL FILE");
            byte[] batchOfBytes = new byte[(int) size];
            int numOfReadBytes = readPartOfFile(fullPath, batchOfBytes, (int) offset, (int) size);
            buf.put(0, batchOfBytes, 0, numOfReadBytes);
            return numOfReadBytes;
        }
    }

    private int getPartitionId(Path fullPath) {
        return Integer.parseInt(fullPath.getFileName().toString());
    }

    /**
     * @param fullPath     path to the file
     * @param batchOfBytes the buffer into which the data is read.
     * @param offset       the start offset in the destination array
     * @param size         the maximum number of bytes read.
     * @return the total number of bytes read into the buffer
     */
    private int readPartOfFile(Path fullPath, byte[] batchOfBytes, int offset, int size) {
        int numOfReadBytes;
        try (FileInputStream fis = new FileInputStream(fullPath.toFile())) {
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
        Path fullPath = getFullPath(root, path);
        log.info("readdir for -> {}", fullPath);

        if (isMatchPattern(fullPath, STREAM_PATTERN)) {
            adminService.getTopicNames(getStreamName(fullPath))
                    .forEach(x -> filter.apply(buf, x, null, 0));
            return 0;
        } else if (isMatchPattern(fullPath, TOPIC_PATTERN)) {
            int numberOfPartitions = adminService.getTopicPartitions(getStreamName(fullPath.getParent()), getTopicName(fullPath));
            for (int i = 0; i < numberOfPartitions; i++) {
                filter.apply(buf, Integer.toString(i), null, 0);
            }
            return 0;
        }

        File file = new File(fullPath.toString());
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

    private boolean isMatchPattern(Path path, String pattern) {
        return path.toString().matches(pattern);
    }
}
