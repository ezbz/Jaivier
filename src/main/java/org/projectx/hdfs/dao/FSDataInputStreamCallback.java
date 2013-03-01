package org.projectx.hdfs.dao;

import org.apache.hadoop.fs.FSDataInputStream;

public interface FSDataInputStreamCallback<T> {

  T deserialize(FSDataInputStream fsis);

}
