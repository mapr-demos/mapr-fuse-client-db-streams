package com.mapr.fuse;

import com.mapr.fuse.utils.AttrsUtils;
import com.mapr.fuse.utils.UserUtils;
import com.mapr.streams.StreamDescriptor;
import com.mapr.streams.Streams;
import jnr.ffi.Runtime;
import org.junit.Assert;
import org.junit.Test;
import ru.serce.jnrfuse.struct.FileStat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static ru.serce.jnrfuse.struct.FileStat.*;

public class TestAttrsUtils {

    @Test
    public void decodeModeTest() {
        Set<PosixFilePermission> expexted = new HashSet<>();
        expexted.add(PosixFilePermission.OWNER_READ);
        expexted.add(PosixFilePermission.OWNER_WRITE);
        expexted.add(PosixFilePermission.OWNER_EXECUTE);
        expexted.add(PosixFilePermission.GROUP_READ);
        expexted.add(PosixFilePermission.GROUP_WRITE);
        expexted.add(PosixFilePermission.GROUP_EXECUTE);
        expexted.add(PosixFilePermission.OTHERS_READ);
        expexted.add(PosixFilePermission.OTHERS_WRITE);
        expexted.add(PosixFilePermission.OTHERS_EXECUTE);

        Set<PosixFilePermission> permissions = AttrsUtils.decodeMode(511);

        Assert.assertEquals(expexted, permissions);
    }

    @Test
    public void setupAttrsTest() throws IOException {
        File file = new File(System.getProperty("user.dir"));
        FileStat stat = new FileStat(Runtime.getSystemRuntime());
        Path path =  file.toPath();
        PosixFileAttributes attrs = Files.getFileAttributeView(path, PosixFileAttributeView.class).readAttributes();

        AttrsUtils.setupAttrs(path, stat);

        Assert.assertEquals(((S_IFDIR + S_IRUSR) | 493), stat.st_mode.intValue());
        Assert.assertEquals(file.length(), stat.st_size.intValue());
        Assert.assertEquals(file.lastModified() / 1000, stat.st_mtim.tv_sec.intValue());
        Assert.assertEquals(UserUtils.getUid(Files.getOwner(path).getName()), stat.st_uid.longValue());
        Assert.assertEquals(UserUtils.getGid(attrs.group().getName()), stat.st_gid.longValue());
        Assert.assertEquals(attrs.creationTime().to(TimeUnit.SECONDS), stat.st_ctim.tv_sec.intValue());
        Assert.assertEquals(attrs.lastAccessTime().to(TimeUnit.SECONDS), stat.st_atim.tv_sec.intValue());
    }

    @Test
    public void setupAttrsStreamTest() throws IOException {
        File streamFile = new File(System.getProperty("user.dir") + "/stream");
        File file = streamFile.getParentFile();
        FileStat stat = new FileStat(Runtime.getSystemRuntime());
        Path path =  streamFile.toPath();
        int topicsAmount = 10;
        StreamDescriptor stream = Streams.newStreamDescriptor();
        stream.setAdminPerms("u:root");
        PosixFileAttributes attrs = Files.getFileAttributeView(path.getParent(),
                PosixFileAttributeView.class).readAttributes();

        AttrsUtils.setupAttrsStream(stream, topicsAmount, path, stat);

        Assert.assertEquals(((S_IFDIR + S_IRUSR) | 384), stat.st_mode.intValue());
        Assert.assertEquals(topicsAmount, stat.st_size.intValue());
        Assert.assertEquals(file.lastModified() / 1000, stat.st_mtim.tv_sec.intValue());
        Assert.assertEquals(0, stat.st_uid.longValue());
        Assert.assertEquals(0, stat.st_gid.longValue());
        Assert.assertEquals(attrs.creationTime().to(TimeUnit.SECONDS), stat.st_ctim.tv_sec.intValue());
        Assert.assertEquals(attrs.lastAccessTime().to(TimeUnit.SECONDS), stat.st_atim.tv_sec.intValue());
    }

    @Test
    public void setupAttrsTopicTest() throws IOException {
        File topicFile = new File(System.getProperty("user.dir") + "/stream/topic");
        File file = topicFile.getParentFile().getParentFile();
        FileStat stat = new FileStat(Runtime.getSystemRuntime());
        Path path =  topicFile.toPath();
        int partitionsAmount = 10;
        StreamDescriptor stream = Streams.newStreamDescriptor();
        stream.setAdminPerms("u:root");
        PosixFileAttributes attrs = Files.getFileAttributeView(path.getParent().getParent(),
                PosixFileAttributeView.class).readAttributes();

        AttrsUtils.setupAttrsTopic(stream, partitionsAmount, path, stat);

        Assert.assertEquals(((S_IFDIR + S_IRUSR) | 384), stat.st_mode.intValue());
        Assert.assertEquals(partitionsAmount, stat.st_size.intValue());
        Assert.assertEquals(file.lastModified() / 1000, stat.st_mtim.tv_sec.intValue());
        Assert.assertEquals(0, stat.st_uid.longValue());
        Assert.assertEquals(0, stat.st_gid.longValue());
        Assert.assertEquals(attrs.creationTime().to(TimeUnit.SECONDS), stat.st_ctim.tv_sec.intValue());
        Assert.assertEquals(attrs.lastAccessTime().to(TimeUnit.SECONDS), stat.st_atim.tv_sec.intValue());
    }

    @Test
    public void setupAttrsPartitionTest() throws IOException {
        File topicFile = new File(System.getProperty("user.dir") + "/stream/topic/0");
        File file = topicFile.getParentFile().getParentFile().getParentFile();
        FileStat stat = new FileStat(Runtime.getSystemRuntime());
        Path path =  topicFile.toPath();
        int partitionSize = 154;
        StreamDescriptor stream = Streams.newStreamDescriptor();
        stream.setAdminPerms("u:root");
        PosixFileAttributes attrs = Files.getFileAttributeView(path.getParent().getParent().getParent(),
                PosixFileAttributeView.class).readAttributes();

        AttrsUtils.setupAttrsPartition(stream, partitionSize, path, stat);

        Assert.assertEquals(((S_IFREG + S_IRUSR) | 384), stat.st_mode.intValue());
        Assert.assertEquals(partitionSize, stat.st_size.intValue());
        Assert.assertEquals(file.lastModified() / 1000, stat.st_mtim.tv_sec.intValue());
        Assert.assertEquals(0, stat.st_uid.longValue());
        Assert.assertEquals(0, stat.st_gid.longValue());
        Assert.assertEquals(attrs.creationTime().to(TimeUnit.SECONDS), stat.st_ctim.tv_sec.intValue());
        Assert.assertEquals(attrs.lastAccessTime().to(TimeUnit.SECONDS), stat.st_atim.tv_sec.intValue());
    }

}
