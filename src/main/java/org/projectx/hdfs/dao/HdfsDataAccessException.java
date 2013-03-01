package org.projectx.hdfs.dao;

import org.springframework.dao.DataAccessException;

public class HdfsDataAccessException extends DataAccessException {
  private static final long serialVersionUID = 1L;

  public HdfsDataAccessException(final String msg, final Throwable cause) {
    super(msg, cause);
  }

}
