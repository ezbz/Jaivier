package org.projectx.hdfs.dao;

import java.util.List;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

/**
 * Provides common read only operations on Hadoop file system
 * 
 * @author erez
 * 
 */
public interface ReadOnlyHdfsDao {
  /**
   * list paths for a given glob style string expression.
   * <p>
   * 
   * See {@link FileSystem#globStatus(org.apache.hadoop.fs.Path)} for more information on globbing.
   * 
   * @param path the glob string representing the path expression
   * @return an array of matching {@link FileStatus} items
   */
  FileStatus[] globStatus(String path);

  /**
   * list paths based on exact path matching 
   * @param path an absolute path 
   * @return an array of child files/directories
   */
  FileStatus[] listStatus(String path);

  /**
   * Get meta-data for a given path
   * @param path an absolute path
   * @return the file system meta data infromation for the path
   */
  FileStatus getFileStatus(String path);

  /**
   * Get summarization of directory, file and size for a given path
   * @param path an abolsute path
   */
  ContentSummary getTotalSummary(String path);

  /**
   * Read the contents of a file to a list of lines. Reads the entire file into memeory (not recommended for large files)
   * @param filePath an absolute path to a file in hdfs
   * @return List of strings representing the entire file
   */
  List<String> readFile(String filePath);

  /**
   * Read the contents of a file using a deserializer capable of handling the input stream 
   * @param filePath an absolute path
   * @param deserializer a deserializer implementation providing a strategy to read the file from a stream
   * @return the value resolved by the deserializer implementation
   */
  <T> T readFile(String filePath, FSDataInputStreamCallback<T> deserializer);

  /**
   * Check if a path exists in hdfs
   * @param path an absolute path
   * @return true if the path exists
   */
  boolean exists(String path);

  void getFolder(String sourceFolder, String targetFolder);

}
