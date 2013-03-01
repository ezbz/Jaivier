package org.projectx.hdfs.dao;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.util.Assert;

public class ReadOnlyHdfsDaoImpl implements ReadOnlyHdfsDao {

  private final FileSystem fileSystem;

  public ReadOnlyHdfsDaoImpl(final FileSystem fileSystem) {
    Assert.notNull(fileSystem, "fileSystem cannot be null");
    this.fileSystem = fileSystem;
  }

  @Override
  public FileStatus[] globStatus(final String path) {
    try {
      return fileSystem.globStatus(new Path(path));
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on list path [" + path + "]", e);
    }
  }

  @Override
  public FileStatus[] listStatus(final String path) {
    try {
      return fileSystem.listStatus(new Path(path));
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on list path [" + path + "]", e);
    }
  }

  @Override
  public FileStatus getFileStatus(final String path) {
    try {
      return fileSystem.getFileStatus(new Path(path));
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on check path exists [" + path + "]", e);
    }
  }

  @Override
  public List<String> readFile(final String filePath) {
    return readFile(filePath, new FSDataInputStreamCallback<List<String>>() {

      @Override
      @SuppressWarnings("unchecked")
      public List<String> deserialize(final FSDataInputStream fsis) {
        try {
          return IOUtils.readLines(fsis);
        } catch (final IOException e) {
          throw new HdfsDataAccessException("on read file [" + filePath + "]", e);
        }
      }
    });
  }

  @Override
  public <T> T readFile(final String filePath, final FSDataInputStreamCallback<T> deserializer) {
    FSDataInputStream fsis = null;
    try {
      fsis = fileSystem.open(new Path(filePath));
      return deserializer.deserialize(fsis);
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on read file [" + filePath + "]", e);
    } finally {
      IOUtils.closeQuietly(fsis);
    }
  }

  @Override
  public ContentSummary getTotalSummary(final String path) {
    try {
      final FileStatus[] fileStatuses = fileSystem.globStatus(new Path(path));
      long dirCount = 0, fileCount = 0, length = 0;
      if (ArrayUtils.isEmpty(fileStatuses)) {
        return new ContentSummary(length, fileCount, dirCount);
      }

      for (final FileStatus fileStatus : fileStatuses) {
        final ContentSummary summary = fileSystem.getContentSummary(fileStatus.getPath());
        dirCount += summary.getDirectoryCount();
        fileCount += summary.getFileCount();
        length += summary.getLength();
      }
      return new ContentSummary(length, fileCount, dirCount);
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on get summary path [" + path + "]", e);
    }
  }

  @Override
  public boolean exists(final String path) {
    try {
      return fileSystem.exists(new Path(path));
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on check path exists [" + path + "]", e);
    }
  }

  @Override
  public void getFolder(final String sourceFolder, final String targetFolder) {
    try {
      fileSystem.copyToLocalFile(new Path(sourceFolder), new Path(targetFolder));
    } catch (final IOException e) {
      throw new HdfsDataAccessException(String.format("on getFolder src=%s target=%s", sourceFolder, targetFolder), e);
    }

  }

}
