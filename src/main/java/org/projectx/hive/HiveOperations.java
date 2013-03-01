package org.projectx.hive;

import java.util.List;

import org.apache.hadoop.hive.service.HiveClient;
import org.projectx.hive.connection.HiveConnectionCallback;
import org.projectx.hive.session.HiveSessionParams;
import org.springframework.dao.DataAccessException;

/**
 * An interface for common hive operations
 * 
 * @author erez
 * 
 */
public interface HiveOperations {
  /**
   * Query for a simple list of strings (equivalent to HiveClient execute query
   * method
   * 
   * @param query
   *          the hiveQL query to execute
   * @return a {@link List} of String each item in the list represents a
   *         delimited line in the result
   */
  List<String> queryForRawList(String query);

  /**
   * Run a query and create a list of results using the provided RowMapper
   * 
   * @param query
   * @param mapper
   * @return
   */
  <T> List<T> queryForList(String query, RowMapper<T> mapper);

  <T> List<T> queryForList(String query, RowMapper<T> mapper, HiveSessionParams sessionParams);

  <T> List<T> queryForList(String query, RowMapper<T> mapper, int maxRows);

  /**
   * Run a query and create a list of results using the provided RowMapper,
   * returning a maximum of maxRows
   * 
   * @param query
   * @param mapper
   * @param maxRows
   * @return
   */
  <T> List<T> queryForList(String query, RowMapper<T> mapper, int maxRows,
      HiveSessionParams sessionParams);

  <T> T execute(HiveConnectionCallback<T> callback);

  /**
   * Execute some code in the context of a HiveClient
   * 
   * @param callback
   * @return
   */
  <T> T execute(HiveConnectionCallback<T> callback, HiveSessionParams sessionParams);

  /**
   * Execute a batch query using a callback. Correlates
   * {@link HiveClient#fetchN(int)} operation where a batch of records is
   * fetched and operated on using the {@link HiveBatchCallback} interface until
   * no more records are available.
   * 
   * @param query
   *          hiveQL query to execute
   * @param batchSize
   *          the number of records to retrieve in each iteration
   * @param callback
   *          a {@link HiveBatchCallback} to operate on each fetched batch
   */
  void executeBatch(String query, int batchSize, HiveBatchCallback<String> callback);

  /**
   * Execute a batch query using a callback and predefined session params
   * 
   * @see #executeBatch(String, int, HiveBatchCallback)
   */
  void executeBatch(String query, int batchSize, HiveBatchCallback<String> callback,
      HiveSessionParams sessionParams);

  /**
   * A typed version of {@link #executeBatch(String, int, HiveBatchCallback)}
   * which accepts a {@link RowMapper} implementation which maps results to
   * typed objects
   */
  <T> void executeBatch(String query, int batchSize, HiveBatchCallback<T> callback,
      RowMapper<T> mapper);

  /**
   * Execute a batch query using predefined session params
   * 
   * @see #executeBatch(String, int, HiveBatchCallback, RowMapper)
   */
  <T> void executeBatch(String query, int batchSize, HiveBatchCallback<T> callback,
      RowMapper<T> mapper, HiveSessionParams sessionParams);

  /**
   * Execute a statement in hive, not returning any results.
   * 
   * @param sql
   * @throws DataAccessException
   */
  void execute(String sql);

  /**
   * Execute a statement in hive after setting session parameters, not returning
   * any results.
   * 
   * @param sql
   * @param sessionParams
   * @throws DataAccessException
   */
  void execute(String sql, HiveSessionParams sessionParams) throws DataAccessException;
}