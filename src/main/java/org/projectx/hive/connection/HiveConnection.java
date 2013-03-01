package org.projectx.hive.connection;

import java.util.List;

import org.apache.hadoop.hive.service.HiveClient;
import org.apache.thrift.transport.TSocket;


/**
 * An abstraction around Hive-based operations providing a common connection style interface to Hive Thrift connection 
 * which typically involves a {@link TSocket} implementation and {@link HiveClient}
 * 
 * @author erez
 *
 */
public interface HiveConnection {
  /**
   * Flush the connection  
   * @see {@link TSocket#flush()}
   */
  void flush();

  /**
   * Close the connection
   * @see {@link TSocket#close()}
   */
  void close();

  /**
   * Check if the connection is open
   * @see {@link TSocket#isOpen()}
   */
  boolean isOpen();

  /**
   * Open the connection
   * @see {@link TSocket#open()}
   */
  void open();

  /**
   * Modify the timeout on the connection
   * @param timeout the timeout of the connection in milliseconds
   * @see {@link TSocket#setTimeout(int)}
   */
  void setTimeout(int timeout);

  /**
   * Execute a hiveQL query
   * @param hiveQL the hiveQL query to execute
   * @see {@link HiveClient#execute(String)}
   */
  void execute(String hiveQL);

  /**
   * Execute a generic hive operation using a {@link HiveClientCallback} interface and returning a type result
   * @return a typed result T
   */
  <T> T execute(HiveClientCallback<T> callback);

  /**
   * Fetch a batch from the previously executed query
   * @param batchSize the batch size to retrieve
   * @return a list of result from the batch
   * @see #execute(HiveClientCallback)
   * @see #execute(String)
   * @see {@link HiveClient#fetchN(String)}
   */
  List<String> fetchBatch(int batchSize);

  /**
   * Fetch a all the results from a previously executed query
   * @return a list of result from the batch
   * @see #execute(HiveClientCallback)
   * @see #execute(String)
   * @see {@link HiveClient#fetchAll()}
   */
  List<String> fetchAll();

}
