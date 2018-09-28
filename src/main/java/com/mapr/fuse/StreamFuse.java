package com.mapr.fuse;

import com.mapr.fuse.client.TopicWriter;
import com.mapr.fuse.service.AdminTopicService;
import com.mapr.fuse.service.ReadDataService;
import com.mapr.fuse.utils.AttrsUtils;
import com.mapr.fuse.utils.ConvertUtils;
import jnr.ffi.Pointer;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;

import static com.mapr.fuse.ErrNo.*;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

public class StreamFuse extends FuseStubFS {
    private static final Pattern TABLE_LINK_PATTERN = Pattern.compile("mapr::table::[0-9.]+");
    private static final Logger log = org.slf4j.LoggerFactory.getLogger(StreamFuse.class);

    private final Path root;
    private ReadDataService tdService;
    private AdminTopicService adminService;
    private TopicWriter topicWriter;

    // exposed for testing
    StreamFuse(Path root, ReadDataService tdService, TopicWriter topicWriter,
                       AdminTopicService adminService) {
        this.root = root;
        this.tdService = tdService;
        this.topicWriter = topicWriter;
        this.adminService = adminService;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 2) {
            String root = args[0];
            String mountPoint = args[1];

            log.info("Mount point -> {}", mountPoint);
            log.info("Root folder -> {}", root);

            TopicWriter writer = new TopicWriter();
            ReadDataService readDataService = new ReadDataService();
            AdminTopicService adminService = new AdminTopicService(new Configuration());
            StreamFuse stub = new StreamFuse(Paths.get(root), readDataService, writer, adminService);
            stub.mount(Paths.get(mountPoint), true);
            stub.umount();
        } else {
            log.error("Mount point and root dirs aren't specified");
            throw new IllegalArgumentException("Usage: mirage-fs <path-to-mapr-fs> <mirage-mount-point>");
        }
    }

    // exposed for testing
    int getPartitionSize(Path stream, String topic, int partitionId) throws IOException {
        if (stream == null || topic == null) {
            return 0;
        } else {
            return tdService.requestTopicSizeData(stream.toString(),
                ConvertUtils.transformToTopicName(stream, topic),
                partitionId);
        }
    }

    // exposed for testing
    boolean isPartitionExists(Path stream, String topic, Integer partitionId) throws IOException {
        return adminService.getTopicPartitions(stream, topic) > partitionId;
    }

    // exposed for testing
    boolean isStreamExists(Path path) throws IOException {
        return path != null && adminService.streamExists(path);
    }

    // exposed for testing
    boolean isTopicExists(Path path) throws IOException {
        return path != null && adminService.getTopicNames(path.getParent())
                .contains(ConvertUtils.getTopicName(path));
    }

    // exposed for testing
    ObjectType getObjectType(Path path) throws IOException {
        if (path == null) {
            return ObjectType.WHATEVER;
        } else if (isPartition(path)) {
            return ObjectType.PARTITION;
        } else if (isTopic(path)) {
            return ObjectType.TOPIC;
        } else if (isTableLink(path)) {
            if (isStream(path)) {
                return ObjectType.STREAM;
            } else {
                return ObjectType.TABLE;
            }
        } else if (Files.isDirectory(path, NOFOLLOW_LINKS)) {
            return ObjectType.DIRECTORY;
        } else if (Files.isSymbolicLink(path)) {
            return ObjectType.LINK;
        } else if (Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)){
            return ObjectType.FILE;
        } else {
            return ObjectType.WHATEVER;
        }
    }

    boolean isTableLink(Path path) throws IOException {
        return path != null && Files.isSymbolicLink(path) &&
                (TABLE_LINK_PATTERN.matcher(Files.readSymbolicLink(path).toString()).matches());
    }

    // exposed for testing
    boolean isStream(Path path) throws IOException {
        try {
            return path != null && isTableLink(path) && isStreamExists(path);
        } catch (UnsupportedOperationException e) {
            log.error("   illegal state exception {}", e.getMessage());
            throw new IllegalStateException("Can't happen", e);
        } catch (SecurityException e) {
            log.info("Can't access {}", path);
            throw new AccessDeniedException(path.toString());
        }
    }

    boolean isTopic(Path path) throws IOException {
        return path != null && isStream(path.getParent());
    }

    boolean isPartition(Path path) throws IOException {
        return path != null && isTopic(path.getParent());
    }

    @Override
    public int getattr(final String path, final FileStat stat) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("Get attr for -> {}", fullPath);

        try {
            ObjectType t = getObjectType(fullPath);
            log.info(" found object of type {}", t);
            switch (t) {
                case STREAM:
                    log.info("   {} is a stream", fullPath);
                    AttrsUtils.setupAttrsStream(
                            adminService.getStreamDescriptor(fullPath),
                            adminService.countTopics(fullPath),
                            fullPath, stat);
                    return 0;

                case TOPIC:
                    if (isTopicExists(fullPath)) {
                        log.info("   {} is a topic", fullPath);
                        AttrsUtils.setupAttrsTopic(
                                adminService.getStreamDescriptor(fullPath.getParent()),
                                adminService.getTopicPartitions(fullPath.getParent(), ConvertUtils.getTopicName(fullPath)),
                                fullPath, stat);
                        log.info("   topic attributes: {}", fullPath, AttrsUtils.attributeToString(stat));
                        return 0;
                    } else
                        log.info("    {} topic not found", fullPath);
                    return ErrNo.ENOENT;

                case PARTITION:
                    log.info("   {} is a partition", fullPath);
                    int partitionId = ConvertUtils.getPartitionId(fullPath);
                    String topic = ConvertUtils.getTopicName(fullPath.getParent());
                    if (!isPartitionExists(fullPath.getParent().getParent(), topic, partitionId)) {
                        log.info("    Partition does not exist {} / {} / {}",
                                fullPath.getParent().getParent(), topic, partitionId);
                        return ENOENT;
                    }
                    log.info("  Attributes from {} / {} / {}",
                            fullPath.getParent().getParent(), topic, partitionId);
                    int size = getPartitionSize(fullPath.getParent().getParent(), topic, partitionId);
                    AttrsUtils.setupAttrsPartition(adminService.getStreamDescriptor(
                            fullPath.getParent().getParent()), size, fullPath, stat);
                    log.info("  partition attributes: {}", AttrsUtils.attributeToString(stat));
                    return 0;

                case DIRECTORY:
                case FILE:
                case LINK:
                case TABLE:
                    log.info("   {} is something ordinary", fullPath);
                    if (Files.exists(fullPath)) {
                        AttrsUtils.setupAttrs(fullPath, stat);
                        return 0;
                    } else {
                        log.info("    {} not found", fullPath);
                        return ENOENT;
                    }
                default:
                    log.info("    {} is something ({}) that we don't understand", fullPath, t);
                    return EINVAL;
            }
        } catch (AccessDeniedException e) {
            log.info("Access denied to {}", fullPath);
            return EACCES;
        } catch (NoSuchFileException e) {
            log.info("Object vanished before we get see it {}", fullPath);
            return EEXIST;
        } catch (IOException e) {
            log.info("I/O exception accessing {}", fullPath);
            return EIO;
        }
    }

    @Override
    public int mkdir(final String path, final long mode) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("mkdir for -> {}", fullPath);

        try {
            ObjectType objectType = getObjectType(fullPath.getParent());
            switch (objectType) {
                case DIRECTORY:
                    try {
                        Files.createDirectory(fullPath, PosixFilePermissions.asFileAttribute(AttrsUtils.decodeMode(mode)));
                        return 0;
                    } catch (NoSuchFileException e) {
                        return ENOENT;
                    } catch (DirectoryNotEmptyException e) {
                        return ENOTEMPTY;
                    } catch (SecurityException e) {
                        return EPERM;
                    } catch (IOException e) {
                        return EIO;
                    }
                case STREAM:
                    try {
                        adminService.createTopic(fullPath.getParent(),
                                ConvertUtils.getTopicName(fullPath));
                        return 0;
                    } catch (NoSuchFileException e) {
                        log.info("Create topic failed {}", e.getMessage());
                        return ENOENT;
                    } catch (SecurityException e) {
                        log.info("Create topic failed {}", e.getMessage());
                        return EPERM;
                    } catch (IOException e) {
                        log.info("Create topic failed {}", e.getMessage());
                        return EIO;
                    }
                case TOPIC:
                    log.info("Attempt to create directory in topic");
                    return EPERM;
                case PARTITION:
                    log.info("Attempt to create directory in partition");
                    return ENOTDIR;
                default:
                    log.info("Attempt to create directory in {}", objectType);
                    return EINVAL;
            }
        } catch (IOException e) {
            StringBuilder trace = new StringBuilder();
            StackTraceElement[] stack = e.getStackTrace();
            for (int i = 0; i < 3; i++) {
                trace.append(stack[i]).append(" // ");
            }
            log.info("I/O exception trying to get object type for {} {}", fullPath, trace);
            return EIO;
        }
    }

    @Override
    public int rmdir(final String path) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("rmdir for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case DIRECTORY:
                    try {
                        Files.delete(fullPath);
                        return 0;
                    } catch (NoSuchFileException e) {
                        return ENOENT;
                    } catch (DirectoryNotEmptyException e) {
                        return ENOTEMPTY;
                    } catch (SecurityException e) {
                        return EPERM;
                    } catch (IOException e) {
                        return EIO;
                    }
                case STREAM:
                    try {
                        adminService.removeStream(fullPath);
                        return 0;
                    } catch (IOException e) {
                        log.info("Remove stream failed {}", e.getMessage());
                        return EIO;
                    }
                case TOPIC:
                    try {
                        adminService.removeTopic(fullPath.getParent(),
                                ConvertUtils.getTopicName(fullPath));
                        return 0;
                    } catch (IOException e) {
                        log.info("Remove topic failed {}", e.getMessage());
                        return EIO;
                    }
                case PARTITION:
                    return EPERM;
                default:
                    return ENOTDIR;
            }
        } catch (IOException e) {
            log.info("I/O error {}", e.getMessage());
            return EIO;
        }
    }

    @Override
    public int chmod(final String path, final long mode) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("chmod for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case DIRECTORY:
                case FILE:
                    Files.setPosixFilePermissions(fullPath, AttrsUtils.decodeMode(mode));
                    return 0;
                case TABLE:
                case STREAM:
                case PARTITION:
                case TOPIC:
                case LINK:
                default:
                    return EINVAL;
            }
        } catch (NoSuchFileException e) {
            return ENOENT;
        } catch (SecurityException e) {
            return EPERM;
        } catch (IOException e) {
            return EIO;
        }
    }

    @Override
    public int chown(final String path, final long uid, final long gid) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("chown for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case DIRECTORY:
                case FILE:
                    AttrsUtils.setUidAndGid(fullPath, uid, gid);
                    return 0;

                case TABLE:
                case STREAM:
                case PARTITION:
                case TOPIC:
                case LINK:
                default:
                    return EINVAL;
            }
        } catch (NoSuchFileException e) {
            return ENOENT;
        } catch (SecurityException e) {
            return EPERM;
        } catch (IOException e) {
            return EIO;
        }
    }

    @Override
    public int truncate(String path, long size) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("truncate for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case FILE:
                    try (SeekableByteChannel f = Files.newByteChannel(fullPath, StandardOpenOption.WRITE)) {
                        f.truncate(size);
                        return 0;
                    }
                case DIRECTORY:
                case TABLE:
                case STREAM:
                case PARTITION:
                case TOPIC:
                case LINK:
                default:
                    return EINVAL;
            }
        } catch (NoSuchFileException e) {
            return ENOENT;
        } catch (SecurityException e) {
            return EPERM;
        } catch (IOException e) {
            return EIO;
        }
    }

    @Override
    public int open(final String path, final FuseFileInfo fi) {
        log.info("open for -> {}", ConvertUtils.getFullPath(root, path));
        return 0;
    }

    @Override
    public int read(final String path, final Pointer buf, final long size, final long offset, final FuseFileInfo fi) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
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
            String topic = ConvertUtils.getTopicName(fullPath.getParent());
            TopicPartition partition =
                    new TopicPartition(
                            ConvertUtils.transformToTopicName(fullPath.getParent().getParent(), topic),
                            ConvertUtils.getPartitionId(fullPath));

            byte[] vr = tdService.readRequiredBytesFromTopicPartition(partition, offset, amountOfBytes, 2000L);

            buf.put(0, vr, 0, vr.length);
            return vr.length;
        } else {
            log.info("read NORMAL FILE");
            byte[] batchOfBytes = new byte[(int) size];
            try {
                int numOfReadBytes = readPartOfFile(fullPath, batchOfBytes, (int) offset, (int) size);
                buf.put(0, batchOfBytes, 0, numOfReadBytes);
                return numOfReadBytes;
            } catch (RuntimeException ex) {
                return EIO;
            }
        }
    }

    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("write for -> {}", fullPath);

        byte[] bytesToWrite = new byte[(int) size];
        buf.get(0, bytesToWrite, 0, (int) size);
        try {
            switch (getObjectType(fullPath)) {
                case FILE:
                    try (SeekableByteChannel f = Files.newByteChannel(fullPath, StandardOpenOption.WRITE)) {
                        f.position(offset);
                        return f.write(ByteBuffer.wrap(bytesToWrite));
                    }

                case PARTITION:
                    Path stream = fullPath.getParent().getParent();
                    Path topic = fullPath.getParent();
                    topicWriter.writeToTopic(
                            ConvertUtils.transformToTopicName(stream, ConvertUtils.getTopicName(topic)),
                            validateBytes(bytesToWrite), 5000L);
                    return (int) size;
                case TABLE:
                case DIRECTORY:
                case STREAM:
                case TOPIC:
                case LINK:
                default:
                    return EINVAL;
            }
        } catch (ExecutionException | TimeoutException e) {
            e.printStackTrace();
            return EIO;
        } catch (NoSuchFileException e) {
            return ENOENT;
        } catch (SecurityException e) {
            return EPERM;
        } catch (IOException e) {
            return EIO;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return EINTR;
        }
    }

    private byte[] validateBytes(byte[] bytesToWrite) {
        return new String(bytesToWrite).replace("\n", "").replace("\r", "").getBytes();
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
            log.error("Problems with reading file");
            throw new RuntimeException(e);
        }
        return numOfReadBytes;
    }

    @Override
    public int opendir(final String path, final FuseFileInfo fi) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("opendir for -> {}", fullPath);
        return super.opendir(fullPath.toString(), fi);
    }

    @Override
    public int readdir(final String path, final Pointer buf,
                       final FuseFillDir filter, final long offset, final FuseFileInfo fi) {
        Path fullPath = ConvertUtils.getFullPath(root, path);
        log.info("readdir for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case STREAM:
                    log.info("  found stream at {}", fullPath);
                    Set<String> topicNames = adminService.getTopicNames(fullPath);
                    topicNames.forEach(x -> log.info("  {}", x));
                    topicNames.forEach(x -> filter.apply(buf, x, null, 0));
                    return 0;
                case TOPIC:
                    log.info("  found topic at {}", fullPath);
                    int numberOfPartitions = adminService.getTopicPartitions(
                            fullPath.getParent(),
                            ConvertUtils.getTopicName(fullPath));
                    for (int i = 0; i < numberOfPartitions; i++) {
                        filter.apply(buf, Integer.toString(i), null, 0);
                    }
                    return 0;
                case DIRECTORY:
                    log.info("  real directory at {}", fullPath);
                    filter.apply(buf, ".", null, 0);
                    filter.apply(buf, "..", null, 0);

                    Files.list(fullPath)
                            .forEach(x -> filter.apply(buf, x.getFileName().toString(), null, 0));
                    return 0;
                case LINK:
                    // follow symbolic links
                    return readdir(fullPath.toRealPath().toString(), buf, filter, offset, fi);
                default:
                    log.error("Can't happen... neither fish nor fowl in readdir for {}", fullPath);
            }

        } catch (AccessDeniedException e) {
            log.info("Permission denied for {}", fullPath);
            return EACCES;
        } catch (IOException e) {
            StringBuilder trace = new StringBuilder();
            StackTraceElement[] stack = e.getStackTrace();
            for (int i = 0; i < 3 && i < stack.length; i++) {
                trace.append(stack[i]).append(" // ");
            }
            log.info("I/O error {} {}", fullPath, trace);
            return EIO;
        }

        // not a stream or topic
        return 0;
    }

    @Override
    public int access(final String path, final int mask) {
        return super.access(ConvertUtils.getFullPath(root, path).toString(), mask);
    }
}
