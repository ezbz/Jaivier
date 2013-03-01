package org.projectx.hive;

import org.springframework.dao.DataAccessException;

/**
 * A generalized runtime exception extension of the {@link DataAccessException} exception used to wrap Hive Execution exceptions
 * 
 * @author erez
 *
 */
public class HiveDataAccessException extends DataAccessException {
  private static final long serialVersionUID = 1L;

  public HiveDataAccessException(final String msg) {
    super(msg);
  }

  public HiveDataAccessException(final String msg, final Throwable cause) {
    super(msg, cause);
  }
}
