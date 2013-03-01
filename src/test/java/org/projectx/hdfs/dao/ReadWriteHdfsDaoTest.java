package org.projectx.hdfs.dao;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hdfs.dao.FSDataInputStreamCallback;
import org.projectx.hdfs.dao.FSDataOutputStreamCallback;
import org.projectx.hdfs.dao.HdfsDataAccessException;
import org.projectx.hdfs.dao.ReadOnlyHdfsDao;
import org.projectx.hdfs.dao.ReadWriteHdfsDao;
import org.projectx.hdfs.dao.ReadWriteHdfsDaoImpl;

@RunWith(MockitoJUnitRunner.class)
public class ReadWriteHdfsDaoTest {
  private static final String TEST_PATH = "/unit_test";

  private ReadWriteHdfsDao classUnderTest;

  @Mock
  private ReadOnlyHdfsDao mockRoDao;
  @Mock
  private FileSystem mockFs;

  @Before
  public void before() {
    classUnderTest = new ReadWriteHdfsDaoImpl(mockRoDao, mockFs);
  }

  @Test
  public void testExists() throws IOException {
    classUnderTest.exists(anyString());
    verify(mockRoDao).exists(anyString());
  }

  @Test
  public void testGlobStatus() throws IOException {
    classUnderTest.globStatus(anyString());
    verify(mockRoDao).globStatus(anyString());
  }

  @Test
  public void testListStatus() throws IOException {
    classUnderTest.listStatus(anyString());
    verify(mockRoDao).listStatus(anyString());
  }

  @Test
  public void testReadFile() throws IOException {
    classUnderTest.readFile(anyString());
    verify(mockRoDao).readFile(anyString());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReadFileWithDeserializer() throws IOException {
    classUnderTest.readFile(anyString(), any(FSDataInputStreamCallback.class));
    verify(mockRoDao).readFile(anyString(), any(FSDataInputStreamCallback.class));
  }

  @Test
  public void testGetTotalSummary() throws IOException {
    classUnderTest.getTotalSummary(anyString());
    verify(mockRoDao).getTotalSummary(anyString());
  }

  @Test
  public void testWrite() throws IOException {
    final FSDataOutputStream mockFSDOS = mock(FSDataOutputStream.class);
    when(mockFs.create(any(Path.class), anyBoolean())).thenReturn(mockFSDOS);
    classUnderTest.write(TEST_PATH, "");
    verify(mockFs).create(any(Path.class), anyBoolean());
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testWrite_Exception() throws IOException {
    when(mockFs.create(any(Path.class), anyBoolean())).thenThrow(new IOException());
    classUnderTest.write(TEST_PATH, "");
  }

  @Test
  public void testWriteWithSerializer() throws IOException {
    final FSDataOutputStreamCallback mockSerializer = mock(FSDataOutputStreamCallback.class);
    classUnderTest.write(TEST_PATH, mockSerializer);
    verify(mockSerializer).serialize(any(FSDataOutputStream.class));
    verify(mockFs).create(any(Path.class), anyBoolean());
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testWriteWithSerializer_Exception() throws IOException {
    final FSDataOutputStreamCallback mockSerializer = mock(FSDataOutputStreamCallback.class);
    when(mockFs.create(any(Path.class), anyBoolean())).thenThrow(new IOException());
    classUnderTest.write(TEST_PATH, mockSerializer);
  }

  @Test
  public void testtDelete() throws IOException {
    classUnderTest.delete(TEST_PATH);
    verify(mockFs).delete(any(Path.class), anyBoolean());
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testtDelete_Exception() throws IOException {
    when(mockFs.delete(any(Path.class), anyBoolean())).thenThrow(new IOException());
    classUnderTest.delete(TEST_PATH);
  }

  @Test
  public void testMkdir() throws IOException {
    classUnderTest.mkdir(TEST_PATH);
    verify(mockFs).mkdirs(any(Path.class));
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testMkdir_Exception() throws IOException {
    when(mockFs.mkdirs(any(Path.class))).thenThrow(new IOException());
    classUnderTest.mkdir(TEST_PATH);
  }
}
