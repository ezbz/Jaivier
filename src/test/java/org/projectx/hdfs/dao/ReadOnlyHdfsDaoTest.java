package org.projectx.hdfs.dao;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.projectx.hdfs.dao.FSDataInputStreamCallback;
import org.projectx.hdfs.dao.HdfsDataAccessException;
import org.projectx.hdfs.dao.ReadOnlyHdfsDao;
import org.projectx.hdfs.dao.ReadOnlyHdfsDaoImpl;

@RunWith(MockitoJUnitRunner.class)
public class ReadOnlyHdfsDaoTest {
  private static final String TEST_PATH = "/TEST_PATH";
  private ReadOnlyHdfsDao classUnderTest;
  @Mock
  private FileSystem mockFs;
  @Mock
  private FileStatus mockStatus;

  @Before
  public void before() {
    classUnderTest = new ReadOnlyHdfsDaoImpl(mockFs);
  }

  @Test
  public void testGetStatus() throws IOException {
    when(mockFs.getFileStatus(new Path(TEST_PATH))).thenReturn(mockStatus);
    final FileStatus status = classUnderTest.getFileStatus(TEST_PATH);
    assertNotNull("incorrect exists result", status);
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testGetStatus_exception() throws IOException {
    when(mockFs.getFileStatus(new Path(TEST_PATH))).thenThrow(new IOException());
    classUnderTest.getFileStatus(TEST_PATH);
  }

  @Test
  public void testGlobStatus() throws IOException {
    when(mockFs.globStatus(new Path(TEST_PATH))).thenReturn(new FileStatus[] { mockStatus });
    final FileStatus[] status = classUnderTest.globStatus(TEST_PATH);
    assertEquals("incorrect result size", 1, status.length);
  }

  @Test
  public void testListStatus() throws IOException {
    when(mockFs.listStatus(new Path(TEST_PATH))).thenReturn(new FileStatus[] { mockStatus });
    final FileStatus[] status = classUnderTest.listStatus(TEST_PATH);
    assertEquals("incorrect result size", 1, status.length);
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testGlobStatus_exception() throws IOException {
    when(mockFs.globStatus(new Path(TEST_PATH))).thenThrow(new IOException());
    classUnderTest.globStatus(TEST_PATH);
  }

  @Test
  public void testExists() throws IOException {
    when(mockFs.exists(new Path(TEST_PATH))).thenReturn(true);
    final boolean exists = classUnderTest.exists(TEST_PATH);
    assertTrue("incorrect exists result", exists);
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testExists_exception() throws IOException {
    when(mockFs.exists(new Path(TEST_PATH))).thenThrow(new IOException());
    classUnderTest.exists(TEST_PATH);
  }

  @Test
  public void testRead() throws IOException {
    final FSDataInputStream mockFSIS = mock(FSDataInputStream.class);
    when(mockFs.open(any(Path.class))).thenReturn(mockFSIS);
    @SuppressWarnings("unchecked")
    final FSDataInputStreamCallback<Object> mockDeserializer = mock(FSDataInputStreamCallback.class);
    classUnderTest.readFile(TEST_PATH, mockDeserializer);
    verify(mockFs).open(any(Path.class));
    verify(mockDeserializer).deserialize(mockFSIS);
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testRead_Exception() throws IOException {
    when(mockFs.open(any(Path.class))).thenThrow(new IOException());
    classUnderTest.readFile(TEST_PATH);
  }

  @Test
  public void testGetTotalSummary() throws IOException {
    final FileStatus mockStatus = mock(FileStatus.class);
    final ContentSummary mockCs = mock(ContentSummary.class);
    when(mockFs.getContentSummary(any(Path.class))).thenReturn(mockCs);
    when(mockFs.globStatus(any(Path.class))).thenReturn(new FileStatus[] { mockStatus });
    classUnderTest.getTotalSummary(TEST_PATH);
    verify(mockFs).globStatus(any(Path.class));
    verify(mockFs).getContentSummary(any(Path.class));
  }

  @Test(expected = HdfsDataAccessException.class)
  public void testGetTotalSummary_Exception() throws IOException {
    when(mockFs.globStatus(any(Path.class))).thenThrow(new IOException());
    classUnderTest.getTotalSummary(TEST_PATH);
  }
}
