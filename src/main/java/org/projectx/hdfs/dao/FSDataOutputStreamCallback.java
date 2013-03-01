package org.projectx.hdfs.dao;

import org.apache.hadoop.fs.FSDataOutputStream;

public interface FSDataOutputStreamCallback {

  void serialize(FSDataOutputStream fos);

}
