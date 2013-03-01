package org.projectx.hdfs.dao;

/**
 * Provides common write operations on Hadoop file system
 * 
 * @author erez
 * 
 */
public interface ReadWriteHdfsDao extends ReadOnlyHdfsDao {
  /**
   * Delete a given path in hdfs
   * @param path an absolute path
   */
  void delete(String path);

  /**
   * Create a folder in hdfs for a give path
   * @param path an absolute path
   */
  void mkdir(String path);

  /**
   * Write a string to a file in hdfs
   * @param path an absolute path to the file
   * @param contents the contents of the file
   */
  void write(String path, String contents);

  /**
   * Write a file in hdfs using a serializer strategy for handling the hdfs output stream
   * @param path an absolute path to the file
   * @param serializer a strategy for handling an output stream to hdfs
   */
  void write(String path, FSDataOutputStreamCallback serializer);

}
