package com.mapr.fuse;

import com.mapr.fuse.client.TopicWriter;
import com.mapr.fuse.service.AdminTopicService;
import com.mapr.fuse.service.ReadDataService;
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
import java.io.RandomAccessFile;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static com.mapr.fuse.ErrNo.*;
import static ru.serce.jnrfuse.struct.FileStat.S_IFDIR;
import static ru.serce.jnrfuse.struct.FileStat.S_IFREG;
import static ru.serce.jnrfuse.struct.FileStat.S_IRUSR;

@Slf4j
public class StreamFuse extends FuseStubFS {

    private final static String STREAM_PATTERN = "[^/]+$";
    private final static String TOPIC_PATTERN = "[^/]+/[^/]+$";
    private final static String PARTITION_PATTERN = "[^/]+/[^/]+/.+";

    private final static String TOPIC_NAME_PATTERN = "%s:%s";

    private static final Pattern TABLE_LINK_PATTERN = Pattern.compile("mapr::table::[0-9.]+");

    private static String rootPath;

    private final Path root;
    private ReadDataService tdService;
    private AdminTopicService adminService;
    private TopicWriter topicWriter;

    private StreamFuse(Path root, ReadDataService tdService, TopicWriter topicWriter,
                       AdminTopicService adminService) {
        this.root = root;
        this.tdService = tdService;
        this.topicWriter = topicWriter;
        this.adminService = adminService;
    }

    public static void main(String[] args) {
        if (args.length == 2) {
            String mountPoint = args[1];
            rootPath = args[0];

            log.info("Mount point -> {}", mountPoint);
            log.info("Root folder -> {}", rootPath);

            TopicWriter writer = new TopicWriter();
            ReadDataService readDataService = new ReadDataService();
            AdminTopicService adminService = new AdminTopicService(new Configuration());
            StreamFuse stub = new StreamFuse(Paths.get(rootPath), readDataService, writer, adminService);
            stub.mount(Paths.get(mountPoint), true);
            stub.umount();
        } else {
            log.error("Mount point and root dirs aren't specified");
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
        Path fullPath = getFullPath(root, path);
        log.info("Get attr for -> {}", fullPath);

        try {
            if (isStream(fullPath)) {
                log.info("   {} is a stream", fullPath);
                // TODO this needs to be more subtle about which directory to probe
                setupAttrs("/", stat);
                stat.st_mode.set(S_IFDIR + S_IRUSR);
            } else if (isTopic(fullPath)) {
                // TODO this needs to be more subtle about which directory to probe
                log.info("   {} is a topic", fullPath);
                setupAttrs("/", stat);
                stat.st_mode.set(S_IFDIR + S_IRUSR);
            } else if (isPartition(fullPath)) {
                log.info("   {} is a partition", fullPath);
                if (!isPartitionExist(fullPath.getParent(), Integer.parseInt(fullPath.getFileName().toString()))) {
                    return EEXIST;
                }
                setupAttrs("/", stat);
                stat.st_mode.set(S_IFREG + S_IRUSR);
                stat.st_nlink.set(1);
                stat.st_size.set(getPartitionSize(fullPath));
            } else {
                log.info("   {} is something ordinary", fullPath);
                if (Files.exists(fullPath)) {
                    setupAttrs(path, stat);
                } else {
                    return ErrNo.ENOENT;
                }
            }
        } catch (AccessDeniedException e) {
            return EACCES;
        } catch (IOException e) {
            return EIO;
        }
        return 0;
    }

    private int getPartitionSize(Path fullPath) {
        String stream = getStreamName(fullPath.getParent().getParent());
        String topic = getTopicName(fullPath.getParent());
        return tdService.requestTopicSizeData(stream, transformToTopicName(stream, topic),
                getPartitionId(fullPath));
    }

    private boolean isPartitionExist(Path path, Integer partitionId) {
        return adminService.getTopicPartitions(getStreamName(path.getParent()), getTopicName(path)) > partitionId;
    }

    private boolean isTopicExist(Path fullPath) {
        return adminService.getTopicNames(getStreamName(fullPath.getParent())).contains(getTopicName(fullPath));
    }

    private boolean isStreamExist(Path fullPath) {
        return adminService.streamExists(getStreamName(fullPath));
    }

    private String getStreamName(Path fullPath) {
        return String.format("/%s", fullPath.getFileName().toString());
    }

    private String getTopicName(Path fullPath) {
        return fullPath.getFileName().toString();
    }

    private String transformToTopicName(String stream, String topic) {
        return String.format(TOPIC_NAME_PATTERN, stream, topic);
    }

    private boolean isStream(Path file) throws IOException {
        try {
            if (Files.isSymbolicLink(file)) {
                log.info("Found symbolic link {}", file);
                Path target = Files.readSymbolicLink(file);
                log.info("Linked to {}", target);
                return (TABLE_LINK_PATTERN.matcher(target.getFileName().toString()).matches());
            } else {
                return false;
            }
        } catch (UnsupportedOperationException | NotLinkException e) {
            throw new IllegalStateException("Can't happen", e);
        } catch (SecurityException e) {
            log.info("Can't access {}", file);
            throw new AccessDeniedException(file.toString());
        }
    }

    private boolean isTopic(Path file) throws IOException {
        return isStream(file.getParent());
    }

    private boolean isPartition(Path file) throws IOException {
        return isTopic(file.getParent());
    }

    @Override
    @SneakyThrows
    public int mkdir(final String path, final long mode) {
        Path fullPath = getFullPath(root, path);
        log.info("mkdir for -> {}", fullPath);
        if (isTopic(fullPath)) {
            adminService.createTopic(getStreamName(fullPath.getParent()), fullPath.getFileName().toString());
        } else if (isPartition(fullPath)) {
            // can't create a topic
            return EPERM;
        } else {
            Files.createDirectory(fullPath);
        }
        return 0;
    }

    @Override
    @SneakyThrows
    public int rmdir(final String path) {
        Path fullPath = getFullPath(root, path);
        log.info("rmdir for -> {}", fullPath);
        if (isMatchPattern(fullPath, STREAM_PATTERN) && isStreamExist(fullPath)) {
            adminService.removeStream(getStreamName(fullPath));
        } else if (isMatchPattern(fullPath, TOPIC_PATTERN) && isStreamExist(fullPath.getParent())) {
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
    public int truncate(String path, long size) {
        String fullPath = getFullPath(root, path).toString();
        log.info("truncate for -> {}", fullPath);
        return 0;
    }

    @Override
    public int open(final String path, final FuseFileInfo fi) {
        String fullPath = getFullPath(root, path).toString();
        log.info("open for -> {}", fullPath);
        return 0;
    }

    @Override
    public int read(final String path, final Pointer buf, final long size, final long offset, final FuseFileInfo fi) {
        Path fullPath = getFullPath(root, path);
        log.info("read for -> {}", fullPath);
        boolean isPartition;
        try {
            isPartition = isPartition(fullPath);
        } catch (SecurityException e) {
            return EACCES;
        } catch (IOException e) {
            return EIO;
        }
        if (isPartition) {
            log.info("read partition {}", fullPath);
            long amountOfBytes = offset + size;
            String stream = getStreamName(fullPath.getParent().getParent());
            String topic = getTopicName(fullPath.getParent());

            TopicPartition partition =
                    new TopicPartition(transformToTopicName(stream, topic), getPartitionId(fullPath));

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

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        Path fullPath = getFullPath(root, path);
        log.info("write for -> {}", fullPath);

        byte[] bytesToWrite = new byte[(int) size];
        buf.get(0, bytesToWrite, 0, (int) size);

        if (isMatchPattern(fullPath, PARTITION_PATTERN)) {
            topicWriter.writeToTopic(transformToTopicName(getStreamName(fullPath.getParent().getParent()),
                    getTopicName(fullPath.getParent())), validateBytes(bytesToWrite), 5000L);
        } else {
            writeToPosition(fullPath.toAbsolutePath().toString(), bytesToWrite, offset);
        }
        return (int) size;
    }

    private void writeToPosition(String filename, byte[] data, long position) {
        try (RandomAccessFile writer = new RandomAccessFile(filename, "rw")) {
            writer.seek(position);
            writer.write(data);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    private byte[] validateBytes(byte[] bytesToWrite) {
        return new String(bytesToWrite).replace("\n", "").replace("\r", "").getBytes();
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
            //noinspection ResultOfMethodCallIgnored
            fis.skip(offset);
            numOfReadBytes = fis.read(batchOfBytes, 0, size);
        } catch (IOException e) {
            // TODO should allow the exception. Caller should convert to error number
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

        try {
            if (isStream(fullPath)) {
                log.info("  found stream at {}", fullPath);
                Set<String> topicNames = adminService.getTopicNames(getStreamName(fullPath));
                topicNames.forEach(x -> log.info("  {}", x));
                topicNames.forEach(x -> filter.apply(buf, x, null, 0));
                return 0;
            } else if (isTopic(fullPath)) {
                log.info("  found topic at {}", fullPath);
                int numberOfPartitions = adminService.getTopicPartitions(getStreamName(fullPath.getParent()), getTopicName(fullPath));
                for (int i = 0; i < numberOfPartitions; i++) {
                    filter.apply(buf, Integer.toString(i), null, 0);
                }
                return 0;
            }
        } catch (AccessDeniedException e) {
            log.info("Permission denied for {}", fullPath);
            return EACCES;
        } catch (IOException e) {
            log.info("I/O error {}", fullPath);
            return EIO;
        }

        // not a stream or topic
        File file = new File(fullPath.toString());
        if (file.isDirectory()) {
            log.info("  real directory at {}", fullPath);
            filter.apply(buf, ".", null, 0);
            filter.apply(buf, "..", null, 0);

            Arrays.stream(Objects.requireNonNull(file.listFiles()))
                    .map(File::getName)
                    .forEach(x -> filter.apply(buf, x, null, 0));
        } else {
            log.info("Can't happen... neither fish nor fowl in readdir for {}", fullPath);
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
        return path.toString().matches("^" + rootPath + pattern);
    }
}
