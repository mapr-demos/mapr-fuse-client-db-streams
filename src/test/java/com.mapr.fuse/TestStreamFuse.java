package com.mapr.fuse;

import com.mapr.fuse.client.TopicWriter;
import com.mapr.fuse.service.AdminTopicService;
import com.mapr.fuse.service.ReadDataService;
import com.mapr.fuse.utils.AttrsUtils;
import com.mapr.fuse.utils.ConvertUtils;
import org.junit.*;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.mapr.fuse.ErrNo.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;

@SuppressWarnings("WeakerAccess")
public class TestStreamFuse {
  private static final Logger log = org.slf4j.LoggerFactory.getLogger(StreamFuse.class);

  public final static int TOPIC_SIZE_DATA = 100;
  public final static String STREAM_FOLDER = "stream";
  public final static String TOPIC_NAME = "topic";
  public final static int PARTITION_ID = 0;
  public final static String FOLDER_NAME = "new_folder";
  public final static String FILE_NAME = "new_file";
  public StreamFuse fuse;
  public AdminTopicService adminTopicService;
  public Path root;
  public Path stream;
  public Path table;
  public Path link;

  @Before
  public void init() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
      InstantiationException, IOException {
    root = new File(System.getProperty("user.dir")).toPath();
    stream = root.resolve("stream");
    table = root.resolve("table");
    link = root.resolve("link");

    adminTopicService = getAdminTopicServiceMock();
    fuse = new StreamFuse(root, getReadDataServiceMock(), getTopicWriterMock(), adminTopicService);

    deleteFiles();
    Files.createSymbolicLink(stream, Paths.get("mapr::table::2049.42.1181280"));
    Files.createSymbolicLink(table, Paths.get("mapr::table::2049.42.1181281"));
    Files.createSymbolicLink(link, Paths.get("link"));
  }

  @After
  public void deleteFiles() throws IOException {
    Files.deleteIfExists(stream);
    Files.deleteIfExists(table);
    Files.deleteIfExists(link);
    Files.deleteIfExists(root.resolve(FOLDER_NAME));
    Files.deleteIfExists(root.resolve(FILE_NAME));
  }

  @Test
  public void getPartitionSizeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(TOPIC_SIZE_DATA, fuse.getPartitionSize(root.resolve(STREAM_FOLDER), TOPIC_NAME, PARTITION_ID));
  }

  @Test
  public void isPartitionExistsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isPartitionExists(stream, TOPIC_NAME, PARTITION_ID));
    assertFalse(fuse.isPartitionExists(stream, TOPIC_NAME, PARTITION_ID + 1));
  }

  @Test
  public void isStreamExistsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isStreamExists(root.resolve(STREAM_FOLDER)));
    assertFalse(fuse.isStreamExists(root.resolve(STREAM_FOLDER + "2")));
  }

  @Test
  public void isTopicExistsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isTopicExists(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME)));
    assertFalse(fuse.isTopicExists(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "2")));
  }

  @Test
  public void isTableLinkTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isTableLink(root.resolve(STREAM_FOLDER)));
    assertFalse(fuse.isTableLink(root.resolve(STREAM_FOLDER + "2")));
  }

  @Test
  public void isStreamTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isStream(root.resolve(STREAM_FOLDER)));
    assertFalse(fuse.isStream(root.resolve(STREAM_FOLDER + "2")));
  }

  @Test
  public void isTopicTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isTopic(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME)));
    assertFalse(fuse.isTopic(root.resolve(STREAM_FOLDER + "/2/1")));
  }

  @Test
  public void isPartitionTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertTrue(fuse.isPartition(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID)));
    assertFalse(fuse.isPartition(root.resolve(STREAM_FOLDER + "2")));
  }

  @Test
  public void streamTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.STREAM, fuse.getObjectType(root.resolve(STREAM_FOLDER)));
  }

  @Test
  public void topicTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.TOPIC, fuse.getObjectType(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME)));
  }

  @Test
  public void partitionTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.PARTITION, fuse.getObjectType(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID)));
  }

  @Test
  public void tableTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.TABLE, fuse.getObjectType(table));
  }

  @Test
  public void directoryTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.DIRECTORY, fuse.getObjectType(root.resolve("src")));
  }

  @Test
  public void fileTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.FILE, fuse.getObjectType(root.resolve("gradlew")));
  }

  @Test
  public void linkTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.LINK, fuse.getObjectType(link));
  }

  @Test
  public void garbageTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, IOException {
    assertEquals(ObjectType.WHATEVER, fuse.getObjectType(null));
    assertEquals(ObjectType.WHATEVER, fuse.getObjectType(root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "/0/x")));
  }

  @Test
  public void mkdirTest() throws IOException {

    //Test for directory creation inside file
    assertEquals(EIO, fuse.mkdir("gradlew", 511));

    //Test for directory creation inside partition
    assertEquals(ENOTDIR, fuse.mkdir("/" + STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID + "/0", 511));

    //Test for directory creation inside topic
    assertEquals(EPERM, fuse.mkdir("/" + STREAM_FOLDER + "/" + TOPIC_NAME + "/foo", 511));

    //Test for directory creation inside stream
    assertEquals(0, fuse.mkdir("/" + STREAM_FOLDER + "/" + TOPIC_NAME, 511));
    verify(adminTopicService, times(1)).createTopic(stream, TOPIC_NAME);

    //Test for directory creation inside directory
    assertEquals(0, fuse.mkdir(FOLDER_NAME, 511));
    assertTrue(Files.exists(root.resolve(FOLDER_NAME)));
  }

  @Test
  public void rmdirTest() throws IOException {
    //Test for file
    assertEquals(ENOTDIR, fuse.rmdir("gradlew"));

    //Test for partition
    assertEquals(EPERM, fuse.rmdir("/" + STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID));

    //Test for topic
    assertEquals(0, fuse.rmdir("/" + STREAM_FOLDER + "/" + TOPIC_NAME));
    verify(adminTopicService, times(1)).removeTopic(stream, TOPIC_NAME);

    //Test for stream
    assertEquals(0, fuse.rmdir("/" + STREAM_FOLDER));
    verify(adminTopicService, times(1))
        .removeStream(stream);

    //Test for directory
    Path folder = root.resolve(FOLDER_NAME);
    Files.createDirectories(folder);
    assertEquals(0, fuse.rmdir("/" + FOLDER_NAME));
    assertFalse(Files.exists(folder));
  }

  @Test
  public void chmodTest() throws IOException {
    Path tempFile = Files.createFile(root.resolve(FILE_NAME));
    int permissionsCode = 511;

    assertEquals(0, fuse.chmod(FILE_NAME, permissionsCode));

    Set<PosixFilePermission> expectedPermissions = AttrsUtils.decodeMode(permissionsCode);
    Set<PosixFilePermission> permissions = Files.getFileAttributeView(tempFile, PosixFileAttributeView.class)
        .readAttributes().permissions();

    assertEquals(expectedPermissions, permissions);
  }

  public ReadDataService getReadDataServiceMock() throws IOException {
    ReadDataService tdService = Mockito.mock(ReadDataService.class);

    doAnswer(invocation -> {
      log.info("requestTopicSizeData {} {} {} => {}",
          invocation.getArguments()[0], invocation.getArguments()[1], invocation.getArguments()[2]);
      return TOPIC_SIZE_DATA;
    }).when(tdService).requestTopicSizeData(
        eq(stream.toString()),
        eq(ConvertUtils.transformToTopicName(stream, TOPIC_NAME)),
        eq(PARTITION_ID));

    return tdService;
  }

  public AdminTopicService getAdminTopicServiceMock() throws IOException {
    AdminTopicService admin = Mockito.mock(AdminTopicService.class);

    doAnswer(invocation -> {
      log.info("getTopicPartitions {} {} => {}",
          invocation.getArguments()[0], invocation.getArguments()[1], PARTITION_ID + 1);
      return PARTITION_ID + 1;
    }).when(admin).getTopicPartitions(argThat(new PathMatcher(stream)), eq(TOPIC_NAME));

    when(admin.streamExists(argThat(new PathMatcher(stream))))
        .thenReturn(true);

    when(admin.getTopicNames(argThat(new PathMatcher(stream))))
        .thenReturn(new HashSet<>(Collections.singletonList(TOPIC_NAME)));

    doAnswer(invocation -> {
      log.info("removeTopic stub {}", invocation.getArguments()[0]);
      return null;
    }).when(admin).removeTopic(argThat(new PathMatcher(stream)), eq(TOPIC_NAME));

    doAnswer(invocation -> {
      log.info("removeStream stub {}", invocation.getArguments()[0]);
      return null;
    }).when(admin).removeStream(argThat(new PathMatcher(stream)));

    return admin;
  }

  public TopicWriter getTopicWriterMock() {
    return Mockito.mock(TopicWriter.class);
  }
}
