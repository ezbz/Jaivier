package org.projectx.hive.session;

import java.util.List;

import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.springframework.util.Assert;


/**
 * A factory for creating a configured {@link FailoverHiveSession} which tests connections to hive from a predefined list 
 * of {@link Host} and number of attempts. 
 * 
 * @author erez
 * @see FailoverHiveSession
 */
public class FailoverHiveSessionFactory extends AbstractHiveSessionFactory {
  private final List<Host> hosts;
  private final int timeout;
  private final int connectionTestQueryTimeout;
  private final String connectionTestQuery;
  private final int numAttempts;
  private final HiveConnectionFactory hiveConnectionFactory;

  public FailoverHiveSessionFactory(final HiveConnectionFactory hiveConnectionFactory, final List<Host> hosts, final int timeout,
      final String connectionTestQuery, final int connectionTestQueryTimeout, final int numAttempts) {
    Assert.notNull(hiveConnectionFactory, "hiveConnectionFactory cannot be null");
    Assert.notEmpty(hosts, "hosts cannot be empty");
    Assert.hasText(connectionTestQuery, "connectionTestQuery cannot be empty");
    this.hiveConnectionFactory = hiveConnectionFactory;
    this.hosts = hosts;
    this.timeout = timeout;
    this.connectionTestQuery = connectionTestQuery;
    this.connectionTestQueryTimeout = connectionTestQueryTimeout;
    this.numAttempts = numAttempts;
  }

  @Override
  public HiveSession createSession(final HiveSessionParams sessionParameters) {
    final HiveSession session = new FailoverHiveSession(hiveConnectionFactory, hosts, timeout, connectionTestQuery, connectionTestQueryTimeout,
        numAttempts);
    final HiveConnection connection = session.createConnection();
    setSessionParameters(connection, sessionParameters);
    return session;
  }

}
