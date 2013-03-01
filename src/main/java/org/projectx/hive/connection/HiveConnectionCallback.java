package org.projectx.hive.connection;

/**
 * A callback for executing operations on a {@link HiveConnection} instance returning a typed result
 * @author erez
 *
 * @param <T> the result of the operation on a {@link HiveConnection}
 */
public interface HiveConnectionCallback<T> {

  T doInHive(HiveConnection connection);

}
