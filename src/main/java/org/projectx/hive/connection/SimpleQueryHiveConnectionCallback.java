package org.projectx.hive.connection;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleQueryHiveConnectionCallback implements HiveConnectionCallback<List<String>> {
  private static final Logger logger = LoggerFactory.getLogger(SimpleQueryHiveConnectionCallback.class);

  private final int maxRows;
  private final String query;

  public SimpleQueryHiveConnectionCallback(final String query, final int maxRows) {
    this.query = query;
    this.maxRows = maxRows;
  }

  @Override
  public List<String> doInHive(final HiveConnection connection) {
    logger.debug("Fetching maximum of [{}] rows from hive query [{}]", maxRows, query);
    connection.execute(query);
    return (maxRows > 0) ? connection.fetchBatch(maxRows) : connection.fetchAll();
  }

}
