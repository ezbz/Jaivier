package org.projectx.hive.session;

import org.projectx.hive.Host;
import org.projectx.hive.connection.HiveConnection;
import org.projectx.hive.connection.HiveConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

/**
 * Default implementation of a {@link HiveSessionFactory} interface for creating
 * configured {@link HiveSession} objects.
 * 
 * @author erez
 * 
 */
public class SimpleHiveSessionFactory extends AbstractHiveSessionFactory {
  private static final Logger log = LoggerFactory.getLogger(SimpleHiveSessionFactory.class);

  private final String host;
  private final int port;
  private final int timeout;

  private final HiveConnectionFactory hiveConnectionFactory;

  public SimpleHiveSessionFactory(final HiveConnectionFactory hiveConnectionFactory,
      final String host, final int port, final int timeout) {
    Assert.notNull(hiveConnectionFactory, "hiveConnectionFactory cannot be null");
    Assert.notNull(host, "host cannot be null");
    this.hiveConnectionFactory = hiveConnectionFactory;
    this.host = host;
    this.port = port;
    this.timeout = timeout;
  }

  @Override
  public HiveSession createSession(final HiveSessionParams sessionParameters) {
    log.info("Creating a new hive session");
    final HiveSession session = new SimpleHiveSession(hiveConnectionFactory, new Host(host, port),
        timeout);
    final HiveConnection connection = session.createConnection();
    setSessionParameters(connection, sessionParameters);
    return session;
  }

}
