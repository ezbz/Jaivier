package org.projectx.hive;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.springframework.dao.DataAccessException;

/**
 * Basic hive exception translator, only wraps the exception in a
 * DataRetrievalFailureException, making all exceptions effectively
 * non-transient.
 * 
 * @author erez
 * 
 */
public class SimpleHiveExceptionTranslator implements HiveExceptionTranslator {

  @Override
  public DataAccessException translate(final HiveServerException e) {
    return new HiveDataAccessException("Hive exception occurred", e);
  }

  @Override
  public DataAccessException translate(final TException e) {
    return new HiveDataAccessException("Hive exception occurred", e);
  }

}
