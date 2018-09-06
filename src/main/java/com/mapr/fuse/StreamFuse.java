package com.mapr.fuse;

import com.mapr.fuse.client.TopicWriter;
import com.mapr.fuse.service.AdminTopicService;
import com.mapr.fuse.service.ReadDataService;
import com.mapr.fuse.utils.AttrsUtils;
import jnr.ffi.Pointer;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import ru.serce.jnrfuse.FuseFillDir;
import ru.serce.jnrfuse.FuseStubFS;
import ru.serce.jnrfuse.struct.FileStat;
import ru.serce.jnrfuse.struct.FuseFileInfo;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.Set;
import java.util.regex.Pattern;

import static com.mapr.fuse.ErrNo.*;
import static java.nio.file.LinkOption.NOFOLLOW_LINKS;

@Slf4j
public class StreamFuse extends FuseStubFS {

    private final static String TOPIC_NAME_PATTERN = "%s:%s";

    private static final Pattern TABLE_LINK_PATTERN = Pattern.compile("mapr::table::[0-9.]+");

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

    private static Path getFullPath(Path root, String partial) {
        if (partial.startsWith("/")) {
            partial = partial.substring(1);
        }
        return root.resolve(partial);
    }

    private int getPartitionSize(String stream, String topic, int partitionId) {
        return tdService.requestTopicSizeData(stream, transformToTopicName(stream, topic),
                partitionId);
    }

    private boolean isPartitionExists(Path path, String topic, Integer partitionId) {
        return adminService.getTopicPartitions(path.toString(), topic) > partitionId;
    }

    private boolean isStreamExists(Path path) {
        return adminService.streamExists(getStreamName(path));
    }

    private boolean isTopicExists(Path path) {
        return adminService.getTopicNames(path.getParent().toString()).contains(getTopicName(path));
    }

    private String getStreamName(Path path) {
        return path.toString().replace(root.toString(), "");
    }

    private String getTopicName(Path path) {
        return path.getFileName().toString();
    }

    private String transformToTopicName(String stream, String topic) {
        return String.format(TOPIC_NAME_PATTERN, stream, topic);
    }

    private ObjectType getObjectType(Path file) throws IOException {
        if (isPartition(file)) {
            return ObjectType.PARTITION;
        } else if (isTopic(file)) {
            return ObjectType.TOPIC;
        } else if (isTableLink(file)) {
            if (isStream(file)) {
                return ObjectType.STREAM;
            } else {
                return ObjectType.TABLE;
            }
        } else if (Files.isDirectory(file, NOFOLLOW_LINKS)) {
            return ObjectType.DIRECTORY;
        } else if (Files.isSymbolicLink(file)) {
            return ObjectType.LINK;
        } else {
            return ObjectType.FILE;
        }
    }

    @SneakyThrows
    private boolean isTableLink(Path file) {
        return Files.isSymbolicLink(file) &&
                (TABLE_LINK_PATTERN.matcher(Files.readSymbolicLink(file).toString()).matches());
    }

    private boolean isStream(Path file) throws IOException {
        try {
            return Files.isSymbolicLink(file) && isStreamExists(file);
        } catch (UnsupportedOperationException e) {
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
    public int getattr(final String path, final FileStat stat) {
        Path fullPath = getFullPath(root, path);
        log.info("Get attr for -> {}", fullPath);

        String streamName;

        try {
            switch (getObjectType(fullPath)) {
                case STREAM:
                    log.info("   {} is a stream", fullPath);
                    streamName = getStreamName(fullPath);
                    AttrsUtils.setupAttrsStream(adminService.getStreamDescriptor(streamName),
                            adminService.countTopics(streamName), fullPath, stat);
                    return 0;
                case TOPIC:
                    if(isTopicExists(fullPath)) {
                        log.info("   {} is a topic", fullPath);
                        streamName = getStreamName(fullPath.getParent());
                        AttrsUtils.setupAttrsTopic(adminService.getStreamDescriptor(streamName),
                                adminService.getTopicPartitions(streamName, getTopicName(fullPath)), fullPath, stat);
                        log.info("   topic attributes: {}", fullPath, AttrsUtils.attributeToString(stat));
                        return 0;
                    } else
                        return ErrNo.ENOENT;
                case PARTITION:
                    log.info("   {} is a partition", fullPath);
                    int partitionId = Integer.parseInt(fullPath.getFileName().toString());
                    Path topicPath = fullPath.getParent();
                    Path streamPath = topicPath.getParent();
                    String topic = getTopicName(topicPath);
                    streamName = getStreamName(streamPath);
                    if (!isPartitionExists(streamPath, topic, partitionId)) {
                        log.info("    Partition does not exist {} / {} / {}", streamPath, topic, partitionId);
                        return ENOENT;
                    }
                    log.info("  Attributes from {} / {} / {}", streamPath, topic, partitionId);
                    int size = getPartitionSize(streamName, topic, partitionId);
                    AttrsUtils.setupAttrsPartition(adminService.getStreamDescriptor(streamName), size, fullPath, stat);
                    log.info("  partition attributes: {}", AttrsUtils.attributeToString(stat));
                    return 0;
                case DIRECTORY:
                case FILE:
                case LINK:
                case TABLE:
                    log.info("   {} is something ordinary", fullPath);
                    if (Files.exists(fullPath)) {
                        AttrsUtils.setupAttrs(fullPath, stat);
                    } else {
                        return ENOENT;
                    }
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

        return 0;
    }

    // TODO test
    @Override
    @SneakyThrows
    public int mkdir(final String path, final long mode) {
        Path fullPath = getFullPath(root, path);
        log.info("mkdir for -> {}", fullPath);

        switch (getObjectType(fullPath.getParent())) {
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
                    adminService.createTopic(fullPath.getParent().toString(), fullPath.getFileName().toString());
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
            case PARTITION:
                return EPERM;
            default:
                return EINVAL;
        }
    }

    // TODO test
    @Override
    @SneakyThrows
    public int rmdir(final String path) {
        Path fullPath = getFullPath(root, path);
        log.info("rmdir for -> {}", fullPath);

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
                    adminService.removeStream(fullPath.toString());
                } catch (IOException e) {
                    log.info("Remove stream failed {}", e.getMessage());
                    return EIO;
                }
                return 0;
            case TOPIC:
                try {
                    adminService.removeTopic(fullPath.getParent().toString(), fullPath.getFileName().toString());
                } catch (IOException e) {
                    log.info("Remove topic failed {}", e.getMessage());
                    return EIO;
                }
                return 0;
            case PARTITION:
                return EPERM;
            default:
                return ENOTDIR;
        }
    }

    // TODO test
    @Override
    public int chmod(final String path, final long mode) {
        Path fullPath = getFullPath(root, path);
        log.info("chown for -> {}", fullPath);

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

    // TODO check and fix
    @Override
    public int chown(final String path, final long uid, final long gid) {
        Path fullPath = getFullPath(root, path);
        log.info("chown for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case DIRECTORY:
                case FILE:
                    Files.setAttribute(fullPath, "unix:uid", Long.valueOf(uid).intValue());
                    Files.setAttribute(fullPath, "unix:gid", Long.valueOf(gid).intValue());
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

    // TODO test
    @Override
    public int truncate(String path, long size) {
        Path fullPath = getFullPath(root, path);
        log.info("truncate for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case FILE:
                    // TODO probably need attribute here to allow write
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

    // TODO check and fix
    @Override
    public int open(final String path, final FuseFileInfo fi) {
        String fullPath = getFullPath(root, path).toString();
        log.info("open for -> {}", fullPath);
        return 0;
    }

    // TODO check and fix
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

    // TODO test
    @Override
    public int write(String path, Pointer buf, long size, long offset, FuseFileInfo fi) {
        Path fullPath = getFullPath(root, path);
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
                    topicWriter.writeToTopic(transformToTopicName(getStreamName(fullPath.getParent().getParent()),
                            getTopicName(fullPath.getParent())), validateBytes(bytesToWrite), 5000L);
                    return (int) size;
                case TABLE:
                case DIRECTORY:
                case STREAM:
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

    // TODO check and fix
    private void writeToPosition(String filename, byte[] data, long position) {
        try (RandomAccessFile writer = new RandomAccessFile(filename, "rw")) {
            writer.seek(position);
            writer.write(data);
        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    // TODO check and fix
    private byte[] validateBytes(byte[] bytesToWrite) {
        return new String(bytesToWrite).replace("\n", "").replace("\r", "").getBytes();
    }

    // TODO check and fix
    private int getPartitionId(Path fullPath) {
        return Integer.parseInt(fullPath.getFileName().toString());
    }

    // TODO check and fix

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
    public int readdir(final String path, final Pointer buf,
                       final FuseFillDir filter, final long offset, final FuseFileInfo fi) {
        Path fullPath = getFullPath(root, path);
        log.info("readdir for -> {}", fullPath);

        try {
            switch (getObjectType(fullPath)) {
                case STREAM:
                    log.info("  found stream at {}", fullPath);
                    Set<String> topicNames = adminService.getTopicNames(fullPath.toString());
                    topicNames.forEach(x -> log.info("  {}", x));
                    topicNames.forEach(x -> filter.apply(buf, x, null, 0));
                    return 0;
                case TOPIC:
                    log.info("  found topic at {}", fullPath);
                    int numberOfPartitions = adminService.getTopicPartitions(
                            fullPath.getParent().toString(), fullPath.getFileName().toString());
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
            log.info("I/O error {}", fullPath);
            // TODO deferred clean up. Shouldn't be in real code
            e.printStackTrace();
            return EIO;
        }

        // not a stream or topic
        return 0;
    }

    // TODO check and fix
    @Override
    public int access(final String path, final int mask) {
        String fullPath = getFullPath(root, path).toString();
        return super.access(fullPath, mask);
    }
}
