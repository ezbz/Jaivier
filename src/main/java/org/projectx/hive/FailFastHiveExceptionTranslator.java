package org.projectx.hive;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.springframework.dao.DataAccessException;

/**
 * A fail-fast implementation of the {@link HiveExceptionTranslator} interface,
 * wraps Hive related checked exceptions with a general runtime
 * {@link HiveDataAccessException} to mitigate hive execution failures at
 * low-level operations.
 * 
 * @author Erez
 * 
 */
public class FailFastHiveExceptionTranslator implements HiveExceptionTranslator {

  @Override
  public DataAccessException translate(final HiveServerException e) {
    return new HiveDataAccessException("HiveServerException while executing hive operation", e);
  }

  @Override
  public DataAccessException translate(final TException e) {
    return new HiveDataAccessException("TException while executing hive operation", e);
  }

}
