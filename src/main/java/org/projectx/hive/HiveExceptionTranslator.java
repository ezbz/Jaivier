package org.projectx.hive;

import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.springframework.dao.DataAccessException;

/**
 * An interface for an exception translator which wraps Hive execution checked exceptions with a runtime flavor of {@link DataAccessException}
 * 
 * @author erez
 *
 */
public interface HiveExceptionTranslator {

  DataAccessException translate(HiveServerException e);

  DataAccessException translate(TException e);

}
