package org.projectx.hive.connection;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;

/**
 * A callback interface for operations done with a {@link HiveClient} class. Used to generalize {@link HiveClient} operations which 
 * throw checked exceptions and return a typed value.
 * @author erez
 *
 * @param <T> the type of the result returned by the {@link HiveClient} operation implemented by this callback
 */
public interface HiveClientCallback<T> {

  public T doWithClient(HiveClient client) throws HiveServerException, TException;
}
