package org.projectx.hdfs.dao;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.util.Assert;

public class ReadWriteHdfsDaoImpl implements ReadWriteHdfsDao {

  private final FileSystem fileSystem;
  private final ReadOnlyHdfsDao readOnlyHdfsDao;

  public ReadWriteHdfsDaoImpl(final ReadOnlyHdfsDao readOnlyHdfsDao, final FileSystem fileSystem) {
    Assert.notNull(readOnlyHdfsDao, "readOnlyHdfsDao cannot be null");
    Assert.notNull(fileSystem, "fileSystem cannot be null");
    this.readOnlyHdfsDao = readOnlyHdfsDao;
    this.fileSystem = fileSystem;
  }

  @Override
  public FileStatus[] globStatus(final String path) {
    return readOnlyHdfsDao.globStatus(path);
  }

  @Override
  public FileStatus[] listStatus(final String path) {
    return readOnlyHdfsDao.listStatus(path);
  }

  @Override
  public FileStatus getFileStatus(final String path) {
    return readOnlyHdfsDao.getFileStatus(path);
  }

  @Override
  public List<String> readFile(final String filePath) {
    return readOnlyHdfsDao.readFile(filePath);
  }

  @Override
  public boolean exists(final String path) {
    return readOnlyHdfsDao.exists(path);
  }

  @Override
  public <T> T readFile(final String filePath, final FSDataInputStreamCallback<T> deserializer) {
    return readOnlyHdfsDao.readFile(filePath, deserializer);
  }

  @Override
  public ContentSummary getTotalSummary(final String path) {
    return readOnlyHdfsDao.getTotalSummary(path);
  }

  @Override
  public void write(final String path, final String contents) {
    write(path, new FSDataOutputStreamCallback() {

      @Override
      public void serialize(final FSDataOutputStream fos) {
        try {
          IOUtils.write(contents, fos);
        } catch (final IOException e) {
          throw new HdfsDataAccessException("on delete path [" + path + "]", e);
        }
      }
    });
  }

  @Override
  public void write(final String path, final FSDataOutputStreamCallback serializer) {
    FSDataOutputStream fos = null;
    try {
      fos = fileSystem.create(new Path(path), true);
      serializer.serialize(fos);
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on delete path [" + path + "]", e);
    } finally {
      IOUtils.closeQuietly(fos);
    }
  }

  @Override
  public void delete(final String path) {
    try {
      fileSystem.delete(new Path(path), true);
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on delete path [" + path + "]", e);
    }
  }

  @Override
  public void mkdir(final String path) {
    try {
      fileSystem.mkdirs(new Path(path));
    } catch (final IOException e) {
      throw new HdfsDataAccessException("on create path [" + path + "]", e);
    }
  }

  @Override
  public void getFolder(final String sourceFolder, final String targetFolder) {
    readOnlyHdfsDao.getFolder(sourceFolder, targetFolder);
  }

}
