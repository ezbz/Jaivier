package org.projectx.hive.session;

import java.util.List;

import org.projectx.hive.HiveDataAccessException;
import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;


/**
 * A connection testing, failover implementation of the {@link HiveSession} interface. 
 * <p>
 * Failover hive sessions accept a collection of {@link Host} and 
 * a {@link #setConnectionTestQuery(String) connection test query}. Upon creation of a new hive session the connection is 
 * test using the test query and if an exception is thrown then then a connection is attempted with the next host on the list.
 * If no connection can be obtained after iteration is {@link #setNumAttempts(int) attempted} the factory fails miserably with an 
 * evil exception.
 * <p>
 * A connection timeout can be provided to determine a specific timeout for the test query (separate from the general connection timeout), if the test query is successful then 
 * the original <code>timeout</code> is used for further operations performed on the provided {@link HiveConnection}.
 * @author erez
 *
 */
public class FailoverHiveSession implements HiveSession {

  private final List<Host> hosts;
  private final int timeout;

  private final String connectionTestQuery;
  private final int connectionTestQueryTimeout;

  private final int numAttemptsPerHost;
  private final HiveConnectionFactory hiveConnectionFactory;
  private HiveSessionEventHandler hiveSessionEventHandler = new LoggingHiveSessionEventHandler();

  public FailoverHiveSession(final HiveConnectionFactory hiveConnectionFactory, final List<Host> hosts, final int timeout,
      final String connectionTestQuery, final int connectionTestQueryTimeout, final int numAttemptsPerHost) {
    Assert.notNull(hiveConnectionFactory, "hiveConnectionFactory cannot be null");
    Assert.hasText(connectionTestQuery, "connectionTestQuery cannot be empty");
    Assert.isTrue(numAttemptsPerHost > 0, "the number of configure attempts must be a positive integer");
    this.hiveConnectionFactory = hiveConnectionFactory;
    this.hosts = hosts;
    this.timeout = timeout;
    this.connectionTestQuery = connectionTestQuery;
    this.connectionTestQueryTimeout = connectionTestQueryTimeout;
    this.numAttemptsPerHost = numAttemptsPerHost;
  }

  public void setHiveSessionEventHandler(final HiveSessionEventHandler hiveSessionEventHandler) {
    this.hiveSessionEventHandler = hiveSessionEventHandler;
  }

  @Override
  public HiveConnection createConnection() {
    for (int attempt = 1; attempt <= numAttemptsPerHost; attempt++) {
      for (final Host host : hosts) {
        final HiveConnection connection = hiveConnectionFactory.createHiveConnection(host, timeout);
        try {
          return testConnection(connection, host, attempt);
        } catch (final HiveDataAccessException e) {
          hiveSessionEventHandler.connectFailure(host, attempt, numAttemptsPerHost, e);
          closeSilently(connection);
        }
      }
    }
    throw failedMiserably();
  }

  protected HiveConnection testConnection(final HiveConnection connection, final Host host, final int attempt) {
    hiveSessionEventHandler.connectAttempt(host, attempt, numAttemptsPerHost, connectionTestQuery);
    connection.open();
    connection.setTimeout(connectionTestQueryTimeout);
    connection.execute(connectionTestQuery);
    connection.setTimeout(timeout);
    hiveSessionEventHandler.connectSuccess(host, attempt, numAttemptsPerHost);
    return connection;
  }

  protected void closeSilently(final HiveConnection connection) {
    try {
      connection.close();
    } catch (final HiveDataAccessException e) {
      hiveSessionEventHandler.closeException(e);
    }
  }

  protected HiveDataAccessException failedMiserably() {
    final String message = String.format(
        "All failover options exhausted while trying to connect to hive after [%s] attempts on each server, servers attempted [%s]",
        numAttemptsPerHost, StringUtils.collectionToCommaDelimitedString(hosts));
    hiveSessionEventHandler.connectOptionsExhausted(hosts, numAttemptsPerHost, message);
    return new HiveDataAccessException(message);
  }
}
