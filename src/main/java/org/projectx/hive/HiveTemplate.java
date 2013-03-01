package org.projectx.hive;

import java.util.List;

import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionCallback;
import org.projectx.hive.connection.SimpleQueryHiveConnectionCallback;
import org.projectx.hive.session.HiveSession;
import org.projectx.hive.session.HiveSessionFactory;
import org.projectx.hive.session.HiveSessionParams;
import org.springframework.dao.DataAccessException;
import org.springframework.util.Assert;

/**
 * This is the central access class for Hive. Follows some of the behavior of
 * JdbcTemplate.
 * 
 * @author erez
 * 
 */
public class HiveTemplate implements HiveOperations {

  private final HiveSessionFactory sessionFactory;

  private HiveSessionParams defaultSessionParams = new HiveSessionParams();

  public HiveTemplate(final HiveSessionFactory sessionFactory) {
    Assert.notNull(sessionFactory, "sessionFactory cannot be null");
    this.sessionFactory = sessionFactory;
  }

  public HiveTemplate(final HiveSessionFactory sessionFactory,
      final HiveSessionParams defaultSessionParams) {
    Assert.notNull(sessionFactory, "sessionFactory cannot be null");
    Assert.notNull(defaultSessionParams, "defaultSessionParams cannot be null");
    this.sessionFactory = sessionFactory;
    this.defaultSessionParams = defaultSessionParams;
  }

  public HiveTemplate(final HiveSessionFactory sessionFactory, final char delimiter) {
    this.sessionFactory = sessionFactory;
  }

  @Override
  public List<String> queryForRawList(final String query) {
    return queryForList(query, new SimpleRowMapper());
  }

  @Override
  public <T> List<T> queryForList(final String query, final RowMapper<T> mapper) {
    return queryForList(query, mapper, 0);
  }

  @Override
  public <T> List<T> queryForList(final String query, final RowMapper<T> mapper,
      final HiveSessionParams sessionParams) {
    return queryForList(query, mapper, 0, sessionParams);
  }

  @Override
  public <T> List<T> queryForList(final String query, final RowMapper<T> mapper, final int maxRows) {
    return queryForList(query, mapper, maxRows, new HiveSessionParams());
  }

  @Override
  public <T> List<T> queryForList(final String query, final RowMapper<T> mapper, final int maxRows,
      final HiveSessionParams sessionParams) {
    final List<String> result = execute(new SimpleQueryHiveConnectionCallback(query, maxRows),
        sessionParams);
    final HiveResultExtractor<T> rse = new RowMapperHiveResultExtractor<T>(mapper);
    return rse.extractData(result);
  }

  @Override
  public <T> T execute(final HiveConnectionCallback<T> callback) {
    return execute(callback, new HiveSessionParams());
  }

  @Override
  public <T> T execute(final HiveConnectionCallback<T> callback,
      final HiveSessionParams sessionParams) {
    final HiveSessionParams effectiveParams = createEffectiveSessionParams(sessionParams);
    final HiveSession session = sessionFactory.createSession(effectiveParams);
    final HiveConnection connection = session.createConnection();

    T result;
    try {
      result = callback.doInHive(connection);
    } finally {
      connection.close();
    }
    return result;
  }

  @Override
  public void executeBatch(final String query, final int batchSize,
      final HiveBatchCallback<String> callback, final HiveSessionParams sessionParams) {
    executeBatch(query, batchSize, callback, new SimpleRowMapper(), sessionParams);
  }

  @Override
  public void executeBatch(final String query, final int batchSize,
      final HiveBatchCallback<String> callback) {
    executeBatch(query, batchSize, callback, new SimpleRowMapper(), new HiveSessionParams());
  }

  @Override
  public <T> void executeBatch(final String query, final int batchSize,
      final HiveBatchCallback<T> callback, final RowMapper<T> mapper) {
    executeBatch(query, batchSize, callback, mapper, new HiveSessionParams());
  }

  @Override
  public <T> void executeBatch(final String query, final int batchSize,
      final HiveBatchCallback<T> callback, final RowMapper<T> mapper,
      final HiveSessionParams sessionParams) {
    final HiveSessionParams effectiveParams = createEffectiveSessionParams(sessionParams);
    final HiveSession session = sessionFactory.createSession(effectiveParams);
    final HiveConnection connection = session.createConnection();

    try {
      connection.execute(query);
      final HiveResultExtractor<T> rse = new RowMapperHiveResultExtractor<T>(mapper, batchSize);

      List<String> batch;
      do {
        batch = connection.fetchBatch(batchSize);
        final List<T> transformedBatch = rse.extractData(batch);
        callback.doWithBatch(transformedBatch);
      } while ((null != batch) && !batch.isEmpty());
    } finally {
      connection.close();
    }
  }

  @Override
  public void execute(final String sql) {
    execute(sql, new HiveSessionParams());
  }

  @Override
  public void execute(final String sql, final HiveSessionParams sessionParams)
      throws DataAccessException {

    execute(new HiveConnectionCallback<Void>() {

      @Override
      public Void doInHive(final HiveConnection connection) {
        final String trimmedSql = sql.trim();
        if (!trimmedSql.equals("")) {
          connection.execute(sql);
        }
        return null;
      }

    }, sessionParams);
  }

  private HiveSessionParams createEffectiveSessionParams(final HiveSessionParams sessionParams) {
    final HiveSessionParams effective = new HiveSessionParams(defaultSessionParams);
    effective.update(sessionParams);
    return effective;
  }
}
