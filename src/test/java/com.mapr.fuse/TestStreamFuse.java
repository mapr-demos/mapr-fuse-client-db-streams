package com.mapr.fuse;

import com.mapr.fuse.client.TopicWriter;
import com.mapr.fuse.service.AdminTopicService;
import com.mapr.fuse.service.ReadDataService;
import com.mapr.fuse.utils.ConvertUtils;
import org.junit.*;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashSet;

import static com.mapr.fuse.ErrNo.*;
import static org.mockito.Mockito.*;

public class TestStreamFuse {

  public final static int TOPIC_SIZE_DATA = 100;
  public final static String STREAM_NAME = "/stream";
  public final static String STREAM_FOLDER = "stream";
  public final static String TOPIC_NAME = "topic";
  public final static int PARTITION_ID = 0;
  public final static String FOLDER_NAME = "new_folder";
  public StreamFuse fuse;
  public AdminTopicService adminTopicService;
  public Path root = new File(System.getProperty("user.dir")).toPath();
  public Path stream;
  public Path table;
  public Path link;

  @Before
  public void init() throws NoSuchMethodException, IllegalAccessException, InvocationTargetException,
          InstantiationException, IOException {
      Constructor c = StreamFuse.class.getDeclaredConstructor(Path.class, ReadDataService.class, TopicWriter.class,
            AdminTopicService.class);
      c.setAccessible(true);

      adminTopicService = getAdminTopicServiceMock();
      fuse = (StreamFuse) c.newInstance(root, getReadDataServiceMock(), getTopicWriterMock(), adminTopicService);

      stream = root.resolve("stream");
      table = root.resolve("table");
      link = root.resolve("link");
      destroy();
      Files.createSymbolicLink(stream, Paths.get("mapr::table::2049.42.1181280"));
      Files.createSymbolicLink(table, Paths.get("mapr::table::2049.42.1181281"));
      Files.createSymbolicLink(link, Paths.get("link"));
  }

  @After
  public void destroy() throws IOException {
      Files.deleteIfExists(stream);
      Files.deleteIfExists(table);
      Files.deleteIfExists(link);
      Files.deleteIfExists(root.resolve(FOLDER_NAME));
  }

  @Test
  public void getPartitionSizeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("getPartitionSize", String.class, String.class, int.class);
      method.setAccessible(true);
      int partitionId = Integer.parseInt(method.invoke(fuse, STREAM_NAME, TOPIC_NAME, PARTITION_ID).toString());

      Assert.assertEquals(TOPIC_SIZE_DATA, partitionId);
  }

  @Test
  public void isPartitionExistsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isPartitionExists", String.class, String.class, Integer.class);
      method.setAccessible(true);
      boolean existsPartition = Boolean.valueOf(method.invoke(fuse, STREAM_NAME, TOPIC_NAME, PARTITION_ID).toString());

      Assert.assertTrue(existsPartition);

      boolean notExistsPartition = Boolean.valueOf(method.invoke(fuse, STREAM_NAME, TOPIC_NAME, PARTITION_ID + 1)
              .toString());

      Assert.assertFalse(notExistsPartition);
  }

  @Test
  public void isStreamExistsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isStreamExists", Path.class);
      method.setAccessible(true);
      boolean existsStream = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER)).toString());

      Assert.assertTrue(existsStream);

      boolean notExistsStream = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "2")).toString());

      Assert.assertFalse(notExistsStream);
  }

  @Test
  public void isTopicExistsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isTopicExists", Path.class);
      method.setAccessible(true);
      boolean existsTopic = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME)).toString());

      Assert.assertTrue(existsTopic);

      boolean notExistsTopic = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "2"))
              .toString());

      Assert.assertFalse(notExistsTopic);
  }

  @Test
  public void isTableLinkTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isTableLink", Path.class);
      method.setAccessible(true);
      boolean isTable = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER)).toString());

      Assert.assertTrue(isTable);

      boolean isNotTable = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "2")).toString());

      Assert.assertFalse(isNotTable);
  }

  @Test
  public void isStreamTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isStream", Path.class);
      method.setAccessible(true);
      boolean isStream = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER)).toString());

      Assert.assertTrue(isStream);

      boolean isNotStream = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "2")).toString());

      Assert.assertFalse(isNotStream);
  }

  @Test
  public void isTopicTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isTopic", Path.class);
      method.setAccessible(true);
      boolean isTopic = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME)).toString());

      Assert.assertTrue(isTopic);

      boolean isNotTopic = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "/2/1")).toString());

      Assert.assertFalse(isNotTopic);
  }

  @Test
  public void isPartitionTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("isPartition", Path.class);
      method.setAccessible(true);
      boolean isPartition = Boolean.valueOf(method.invoke(fuse,
              root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID)).toString());

      Assert.assertTrue(isPartition);

      boolean isNotPartition = Boolean.valueOf(method.invoke(fuse, root.resolve(STREAM_FOLDER + "2")).toString());

      Assert.assertFalse(isNotPartition);
  }

  @Test
  public void getObjectTypeTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
      Method method = StreamFuse.class.getDeclaredMethod("getObjectType", Path.class);
      method.setAccessible(true);
      ObjectType type;

      //Test for stream
      type = (ObjectType) method.invoke(fuse, root.resolve(STREAM_FOLDER));
      Assert.assertEquals(ObjectType.STREAM,type);

      //Test for topic
      type = (ObjectType) method.invoke(fuse, root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME));
      Assert.assertEquals(ObjectType.TOPIC,type);

      //Test for partition
      type = (ObjectType) method.invoke(fuse, root.resolve(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID));
      Assert.assertEquals(ObjectType.PARTITION,type);

      //Test for table
      type = (ObjectType) method.invoke(fuse, table);
      Assert.assertEquals(ObjectType.TABLE,type);

      //Test for directory
      type = (ObjectType) method.invoke(fuse, root.resolve("src"));
      Assert.assertEquals(ObjectType.DIRECTORY,type);

      //Test for file
      type = (ObjectType) method.invoke(fuse, root.resolve("gradlew"));
      Assert.assertEquals(ObjectType.FILE,type);

      //Test for link
      type = (ObjectType) method.invoke(fuse, link);
      Assert.assertEquals(ObjectType.LINK,type);
  }

  @Test
  public void mkdirTest() throws IOException {
      int result;

      //Test for directory creation inside file
      result = fuse.mkdir("gradlew", 511);
      Assert.assertEquals(EIO, result);

      //Test for directory creation inside partition
      result = fuse.mkdir(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID + "/0", 511);
      Assert.assertEquals(EPERM, result);

      //Test for directory creation inside topic
      result = fuse.mkdir(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID, 511);
      Assert.assertEquals(EPERM, result);

      //Test for directory creation inside stream
      result = fuse.mkdir(STREAM_FOLDER + "/" + TOPIC_NAME, 511);
      Assert.assertEquals(0, result);
      verify(adminTopicService, times(1)).createTopic(STREAM_NAME, TOPIC_NAME);

      //Test for directory creation inside directory
      result = fuse.mkdir(FOLDER_NAME, 511);
      Assert.assertEquals(0, result);
      Assert.assertTrue(Files.exists(root.resolve(FOLDER_NAME)));
  }

  @Test
  public void rmdirTest() throws IOException {
      int result;

      //Test for file
      result = fuse.rmdir("gradlew");
      Assert.assertEquals(ENOTDIR, result);

      //Test for partition
      result = fuse.rmdir(STREAM_FOLDER + "/" + TOPIC_NAME + "/" + PARTITION_ID);
      Assert.assertEquals(EPERM, result);

      //Test for topic
      result = fuse.rmdir(STREAM_FOLDER + "/" + TOPIC_NAME);
      Assert.assertEquals(0, result);
      verify(adminTopicService, times(1)).removeTopic(STREAM_NAME, TOPIC_NAME);

      //Test for stream
      result = fuse.rmdir(STREAM_FOLDER);
      Assert.assertEquals(0, result);
      verify(adminTopicService, times(1)).removeStream(STREAM_NAME);

      //Test for directory
      Files.createDirectories(root.resolve(FOLDER_NAME));
      result = fuse.rmdir(FOLDER_NAME);
      Assert.assertEquals(0, result);
      Assert.assertFalse(Files.exists(root.resolve(FOLDER_NAME)));
  }

  public ReadDataService getReadDataServiceMock() {
      ReadDataService tdService = Mockito.mock(ReadDataService.class);

      when(tdService.requestTopicSizeData(ArgumentMatchers.matches(STREAM_NAME),
              ArgumentMatchers.matches(ConvertUtils.transformToTopicName(STREAM_NAME, TOPIC_NAME)),
              ArgumentMatchers.eq(PARTITION_ID))).thenReturn(TOPIC_SIZE_DATA);

      return tdService;
  }

  public AdminTopicService getAdminTopicServiceMock() {
      AdminTopicService admin = Mockito.mock(AdminTopicService.class);

      when(admin.getTopicPartitions(ArgumentMatchers.matches(STREAM_NAME), ArgumentMatchers.matches(TOPIC_NAME)))
              .thenReturn(PARTITION_ID + 1);

      when(admin.streamExists(ArgumentMatchers.matches(STREAM_NAME))).thenReturn(true);

      when(admin.getTopicNames(ArgumentMatchers.matches(STREAM_NAME)))
              .thenReturn( new HashSet<>(Collections.singletonList(TOPIC_NAME)));

      return admin;
  }

  public TopicWriter getTopicWriterMock() {
      TopicWriter writer = Mockito.mock(TopicWriter.class);
      return writer;
  }

}
